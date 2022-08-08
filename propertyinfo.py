 
import httpx
import os
import json
import traceback

from worker import WorkerError


""" 
Environment
"""
FBSERVICE = os.getenv('FBSERVICE',"")       # No default
FB_DATABASE = os.getenv('FB_DATABASE',"")   # No default
FB_USER = os.getenv('FB_USER',"")           # No default
FB_PASSWD = os.getenv('FB_PASSWD',"")       # No default

HTTP_HEADER = {"Host":"fb.haninge.se", "Proxy":"fbservice"}      # These must always be set in every call


""" 
The worker
"""
async def propertyinfo(taskvars):
    try:
        async with httpx.AsyncClient(timeout=10, verify=False) as client:
            if 'fastighet' not in taskvars:
                return {'DIGIT_ERROR':"Missing variable 'fastighet'"}
            fastighet = taskvars['fastighet']

            params = {'Database':FB_DATABASE, 'User':FB_USER, 'Password':FB_PASSWD}
            workdict = {}

            params['Beteckning'] = fastighet
            r = await client.get(f"{FBSERVICE}/fbservice/fastighet/search/enkelbeteckning/sorterad", headers=HTTP_HEADER, params=params)     # Get a list of all that match
            fastigheter = r.json()['data']
            if fastigheter is None:     # No match!
                return {'DIGIT_ERROR':"No match!"}

            for f in fastigheter:
                if f['kommun'] == "HANINGE":    # Only grab propertites in Haninge kommun
                    workdict[f['fnr']] = {'name':f['beteckning'],'fnr':f['fnr'],'uuid':f['uuid'],'addresses':[]}    # Start filling upp result

            fnrlist = [f for f in workdict]     # Grab all f-numbers
            # r = await client.post(f"{FBSERVICE}/Fastighet/info/fnr", headers=HTTP_HEADER, params=params, json=fnrlist)    # Get more info on each
            # fastighetsinfo = r.json()['data']        
            fastighetsinfo = await fetchHelper(client, f"{FBSERVICE}/fbservice/Fastighet/info/fnr", HTTP_HEADER, params, fnrlist)    # Get more info on each

            for fi in fastighetsinfo:
                if fi['status'] == "A":
                    del workdict[fi['fnr']]     # Remove all non active ("Avregistrerade")

            fnrlist = [f for f in workdict]
            # r = await client.post(f"{FBSERVICE}/adress/search/fastighet/fnr", headers=HTTP_HEADER, params=params, json=fnrlist)   # Get "arbetsplatser" on all fnumbers
            # arbetsplatslista = r.json()['data']
            arbetsplatslista = await fetchHelper(client, f"{FBSERVICE}/fbservice/adress/search/fastighet/fnr", HTTP_HEADER, params, fnrlist)   # Get "arbetsplatser" on all fnumbers

            platslista = []
            for a in arbetsplatslista:
                fnr = a['fnr']
                for g in a['grupp']:
                    workdict[fnr]['addresses'].append({'locationid':g['adressplatsId']})        # Connect "arbetsplatsID" to properties
                    platslista.append(g['adressplatsId'])       # List of all "arbetsplatser" where we want to have adressses
            # r = await client.post(f"{FBSERVICE}/Adress/plats/adressplatsId", headers=HTTP_HEADER, params=params, json=platslista)
            # address = r.json()['data']
            address = await fetchHelper(client, f"{FBSERVICE}/fbservice/Adress/plats/adressplatsId", HTTP_HEADER, params, platslista)

            # addrlista = {}
            # for a in address:
            #     addrlista[a['adressplatsId']] = {'address':a['adressomrade'].capitalize()+" "+a['adressplats'],'zipcode':a['postnummer'],'city':a['postort'].capitalize()}
            addrlista = {a['adressplatsId']: {'address':a['adressomrade'].capitalize()+" "+a['adressplats'],'zipcode':a['postnummer'],'city':a['postort'].capitalize()} for a in address}

            for fastighet in workdict:      # Merge resjsonult
                for inx in range(len(workdict[fastighet]['addresses'])):
                    if workdict[fastighet]['addresses'][inx]['locationid'] in addrlista:        # Skip if no adress found
                        workdict[fastighet]['addresses'][inx] = workdict[fastighet]['addresses'][inx] | addrlista[workdict[fastighet]['addresses'][inx]['locationid']]  # Requires Python 3.9+

            retval = [v for k,v in workdict.items()]
            # resobj = {'RESOBJ':{'value':json.dumps(retval), 'type':"Object", 'valueInfo':{'objectTypeName':"com.camunda.SomeClass",'serializationDataFormat':"application/json"}}}
            return {'data':retval}

    except httpx.ReadTimeout as e:
        raise WorkerError(f"propertyinfo worker raised httpx.ReadTimeout: {traceback.format_exc()}", retry_in=10)       # Timeout. Try again in 10 seconds
    except Exception as e:
        raise WorkerError(f"propertyinfo worker fatal error: {traceback.format_exc()}", retries=0)       # Unknow error. Don't try again


async def fetchHelper(client,url,headers,params,payload):
    MAX = 500
    res = []
    for inx in range(len(payload)//MAX+1):
        py = payload[inx*MAX:(inx+1)*MAX]
        r= await client.post(url, headers=headers, params=params, json=py)
        res.extend(r.json()['data'])
    return res
