import os
import logging
import asyncio
import uuid
import json
import traceback
import signal
import time

import grpc
from zeebe_grpc import gateway_pb2_grpc
from zeebe_grpc.gateway_pb2 import (
    ActivateJobsRequest,
    CompleteJobRequest,
    FailJobRequest,
    TopologyRequest)


""" 
Environment
"""
ZEEBE_ADDRESS = os.getenv('ZEEBE_ADDRESS',"camunda8-zeebe-gateway:26500")

SIGTERM = False     # Mutable. Set to True when SIGTERM or SIGINT is recieved


"""
Worker loop.
Listens on a topic and asynchronous starts given function with retrieved variables
"""
async def worker_loop(workfunc, topic=None):
    global SIGTERM
    worker_tasks = set()
    worker_id = str(uuid.uuid4().time_low)  # Random worker ID
    signal.signal(signal.SIGINT, signal_handler)        # Catch SIGINT
    signal.signal(signal.SIGTERM, signal_handler)       # and SIGTERM
    if not topic:
        topic = workfunc.__name__   # Use name of function as topic to listen to

    async with grpc.aio.insecure_channel(ZEEBE_ADDRESS) as channel:
        stub = gateway_pb2_grpc.GatewayStub(channel)
        if not await zeebe_is_running(stub):
            return      # Zeebe is not running!
        logging.info(f"Starting worker. Topic={topic}, Worker={worker_id}")

        locktime = 100000   # Jobs should *not* get stuck! 100 seconds lock is just to safeguard against worker crashes, but might cause problems. Jobs are not idempotent...
        poll_time = 20000   # Time in ms to poll Zeebe. Longer than 30 seconds affects pod termination grace period.
        ajr = ActivateJobsRequest(type=topic,worker=worker_id,timeout=locktime,
                                  maxJobsToActivate=1,requestTimeout=poll_time)
        while not SIGTERM:     # Loop until terminated
            logging.debug("Requesting jobs to do")
            async for response in stub.ActivateJobs(ajr):
                for job in response.jobs:
                    task = asyncio.create_task(run_worker(workfunc, job, worker_id, stub))         # Schedule an asynchronous task to handle the load. Don't wait for completion.
                    worker_tasks.add(task)      # Save a reference to the coroutine. This prevents it from beeing garbage collected.
                    task.add_done_callback(worker_tasks.discard)        # Will remove the reference once the task is completed.

    # Time to terminate worker
    logging.info(f"Async workers runnning: {len(worker_tasks)}")
    timeOut = 20    # Max time to wait for workers to complete
    while len(worker_tasks) != 0 and timeOut != 0:
        await asyncio.sleep(1)      # Workers are still running
        timeOut -= 1
    logging.info(f"Terminating worker {topic} ({worker_id}).")


"""
Asynchronous function that calls the worker function and completes the job
Can be more than one of these running
"""
async def run_worker(workfunc, job, worker_id, stub):
    logging.info(f"Got a task to do.      Task={workfunc.__name__}, TaskID={job.key}, Worker={worker_id}")
    logging.debug(f"Retries: {job.retries}  Deadline: {job.deadline}  Custom:{job.customHeaders}")
    if job.retries == 0:
        logging.error(f"Got a canceled job?")       # Don't know why these jobs are active?
        return

    try:
        vars = json.loads(job.variables)        # These variables are from the caller
        worker_vars = json.loads(job.customHeaders)     # These variables are configured in BPMN
        newvars = await workfunc(vars|worker_vars)    # Do the work and get new variables in return

        logging.info(f"Mark task as complete. Task={workfunc.__name__}, TaskID={job.key}, Worker={worker_id}")
        await stub.CompleteJob(CompleteJobRequest(jobKey=job.key, variables=json.dumps(newvars)))   # Mark tas as completed and with new variables

    except WorkerError as e:    # Worker signals some error. Could be temporary (e.retries > 0)
        if e.retries < 0:
            e.retries = job.retries-1       # Decrease the number of allowed retries (set in BPMN)
        logging.error(f"TaskID {job.key} failed with \" {e.errorMessage}\".  Retrying {e.retries} times more.")
        try:
            await stub.FailJob(FailJobRequest(jobKey=job.key, retries=e.retries, errorMessage=e.errorMessage, retryBackOff=e.retryTimeout)) 
        except grpc.aio.AioRpcError as grpc_error:      # This is no good...  :(
            return handle_grpc_errors(grpc_error,"in WorkerError")
    except grpc.aio.AioRpcError as grpc_error:      # Oh, oh...
        return handle_grpc_errors(grpc_error,"in main loop")
    except Exception as e:      # WTF?
        logging.critical(f"Task {job.key} failed fatally {traceback.format_exc()}")
        try:
            await stub.FailJob(FailJobRequest(jobKey=job.key, retries=0, errorMessage="Fatal: "+traceback.format_exc(),retryBackOff=0))
        except grpc.aio.AioRpcError as grpc_error:
            return handle_grpc_errors(grpc_error,"in Exception")


"""
Handle termination of container
Might be a better way to do it
"""
def signal_handler(signal, frame):
    global SIGTERM
    logging.info("Got SIGTERM. Waiting for worker to come out of loop.")
    SIGTERM = True


"""
Check if Zeebe is running
"""
async def zeebe_is_running(stub):
    check_timer = 10
    while check_timer != 0:
        try:
            topology = await stub.Topology(TopologyRequest())   # Check that Zeebe is responding
            return True     # It is!
        except grpc.aio.AioRpcError as grpc_error:
            await asyncio.sleep(1)
            check_timer -= 1
    logging.fatal(f"Zeebe engine at {ZEEBE_ADDRESS} is not responding! Exiting!")
    return False


"""
Worker error class
"""
class WorkerError(Exception):
    def __init__(self, error_message="", retries=-1, retry_in=0):
        self.errorMessage = error_message
        self.retries = retries
        self.retryTimeout = retry_in*1000


"""
gRPC error handling function
"""
def handle_grpc_errors(grpc_error,process_name=""):
    if grpc_error.code() == grpc.StatusCode.NOT_FOUND:# Process not found
        loggtext = f"Camunda process {process_name} not found"
        logging.error(loggtext)
        return  
    if grpc_error.code() == grpc.StatusCode.DEADLINE_EXCEEDED:  # Process timeout
        loggtext = f"Camunda process {process_name} timeout"
        logging.error(loggtext)
        return
    if grpc_error.code() == grpc.StatusCode.UNAVAILABLE:  # Zeebe not respodning
        loggtext = f"Camunda/Zebee @{ZEEBE_ADDRESS} not responding!"
        logging.error(loggtext)
        return
    if grpc_error.code() == grpc.StatusCode.DEADLINE_EXCEEDED:  # ????
        loggtext = f"Camunda/Zebee @{ZEEBE_ADDRESS} DEADLINE_EXCEEDED!"
        logging.error(loggtext)
        return
    loggtext = f"Unknown Camunda error: {grpc_error.code()}"
    logging.fatal(loggtext)
    return
