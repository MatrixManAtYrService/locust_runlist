from locust import HttpLocust, TaskSet, task
from sortedcontainers import SortedDict
import os
import json
import sys
import random
import threading
from time import time, sleep
from datetime import timedelta, datetime
import IPython
from dateutil.parser import parse

login_path="/cos/v1/dashboard/internal/login"
#login_path="/billing/v1/login"
file_path="/home/matt/Repo/elquery/v3merchants_day_fewer"
path_suffix="?expand=billing"

calls = SortedDict()
uuids = set()
rate = 100

print("Loading calls...", end='')

with open(file_path, 'r') as file:
    epoch = datetime.utcfromtimestamp(0)
    ct = 0
    for line in file.readlines():
        ct += 1
        x = json.loads(line.replace("'", '"'))

        # if a requestuuid was already seen, don't add it again
        try:
            uuid = x['requestuuid']
            if uuid in uuids:
                continue
            else:
                uuids.add(uuid)
        except KeyError:
            pass

        # strip timezones
        sample_time_with_tz = parse(x['@timestamp'])
        sample_time = sample_time_with_tz.replace(tzinfo=None)

        # load call into magazine
        call = x['uri']
        calls[(sample_time - epoch).total_seconds()] = call

        # just a status indicator
        if ct % 1000 == 0:
            print('.', end='', flush=True)

print(f' {len(calls)} calls loaded.')

class Run:
    def __init__(self, start, end=None, rate=1):
        self.elapsed = 0
        self.start = start
        self.now = start
        self.end = start
        self.rate = rate
        if end:
            self.elapsed = end - start

    def timestamp(self, now):
        self.elapsed = int(now - self.start)
        self.now = self.start + self.elapsed


# initialize for estimate
simulated = Run(calls.keys()[0], end=calls.keys()[-1])
simulation = Run(time(), end=(time() + (simulated.elapsed / rate)))

print(f'Duration to be simulated: {timedelta(seconds=simulated.elapsed)}')
print(f'Proceeding at {rate}x, you\'ll wait: {timedelta(seconds=simulation.elapsed)}')

report_every = 5
try_count = 0
request_count = 0
previous_moment = None
lock = threading.Lock()

# if we can get a lock on the request magazine, and if unset requests exist that should have been sent by now..
# return the uri to call
# otherwise return N
def try_wait_get_next():
    global simulated
    global simulation
    global previous_moment
    global try_count
    global request_count
    global pending_request_count

    try_count += 1
    if lock.acquire(False):

        # locked on, check for requests
        now = time()

        # reinitialize first run
        if not simulation.start:
            simulation = run(now, None)
            simulated = Run(now, now + simulation.elapsed, rate=rate)

        # update trackers for the new moment
        simulation.timestamp(now)
        simulated.timestamp(simulated.start + simulation.elapsed * rate)

        # get unsent requests that aren't too far in the future
        pending_moments = list(calls.irange(minimum=previous_moment, maximum=simulated.now))
        pending_moment_count = len(pending_moments)

        # if they exist
        if pending_moment_count > 0:
            # take the first one and remove it from the magazine
            next_moment = pending_moments[0]
            uri = calls[next_moment]
            del calls[next_moment]

            # update trackers
            previous_moment = next_moment
            request_count += 1

        # no work to be done
        else:
            uri = None

        # occasional status messages
        if try_count % report_every == 1 or report_every == 1:
            print("---")
            print(f"    Your elapsed seconds      : {timedelta(seconds=simulation.elapsed)}")
            print(f"    Simulated elapsed seconds : {timedelta(seconds=simulated.elapsed)}")
            print(f"    Your time      : {datetime.fromtimestamp(simulation.now).strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"    Simulated time : {datetime.fromtimestamp(simulated.now).strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"    Request Attempts : {try_count}")
            print(f"    Overdue requests : {len(list(pending_moments))}")
            print(f"    Sent requests    : {request_count}")

        lock.release()
        return uri

    else:
        return None

def go():
    uri = try_wait_get_next()
    if uri:
        print(uri, flush=True)

# comment out for running via web interface
# comment in for testing the test runner
for _ in range(0,100):
    sleep(0.5)

    threads = []
    for __ in range(0,4):
        threads.append(threading.Thread(target=go))
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

headers = {'Content-Type' : 'application/json',
           'Accept'       : 'application/json, text/javascript, */*; q=0.01',
           'Connection'   : 'keep-alive'}

# Pull user and pass from environment

def no_user():
    sys.exit("username is undefined, try typing: 'export cos_user=<your_username>'")

def no_pass():
    sys.exit("password is undefined, try typing: 'read -s cos_pass && export cos_pass'")

def get_or_fail(varname, fail_func):
    try:
        it = os.environ[varname]
        if not it:
            fail_func()
    except KeyError:
        fail_func()
    return it

# This class says what each user should do
class Tasks(TaskSet):

    def on_start(self):
        user = get_or_fail('cos_user', no_user)
        pw   = get_or_fail('cos_user', no_pass)
        response=self.client.post(login_path,
                                  headers=headers,
                                  data=json.dumps({'username':user,
                                                   'password':pw}))
        print(f"""{login_path} :[{response.status_code}][{response.text}][cookies: {self.locust.client.cookies.get_dict()}]""")

    @task(1)
    def do_it(self):
        path = try_wait_get_next()
        if path:
            path += path_suffix
            response=self.client.get(path, cookies=self.locust.client.cookies.get_dict())
            print(f"""{path} :[{response.status_code}][{response.text}]""")

# This class says how often each user should do it
class WebsiteUser(HttpLocust):
    task_set = Tasks
    min_wait = 500
    max_wait = 500
