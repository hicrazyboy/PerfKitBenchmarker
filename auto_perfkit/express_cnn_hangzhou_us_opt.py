import logging
import subprocess
import threading
import re
import schedule
import time
import datetime

__author__ = 'gaolong.gl@alibaba-inc.com'
DEFAULT_TIMEOUT = 3000
PKB_PATH = ['/root/PerfKitBenchmarker/pkb.py']
SCHEMA_PATH = ['/root/results_table_schema.json']
RETRIES = 5

def IssueCommand(cmd, force_info_log=False, suppress_warning=False,
                 env=None, timeout=DEFAULT_TIMEOUT, input=None):
  """Tries running the provided command once.

  Args:
    cmd: A list of strings such as is given to the subprocess.Popen()
        constructor.
    force_info_log: A boolean indicating whether the command result should
        always be logged at the info level. Command results will always be
        logged at the debug level if they aren't logged at another level.
    suppress_warning: A boolean indicating whether the results should
        not be logged at the info level in the event of a non-zero
        return code. When force_info_log is True, the output is logged
        regardless of suppress_warning's value.
    env: A dict of key/value strings, such as is given to the subprocess.Popen()
        constructor, that contains environment variables to be injected.
    timeout: Timeout for the command in seconds. If the command has not finished
        before the timeout is reached, it will be killed. Set timeout to None to
        let the command run indefinitely. If the subprocess is killed, the
        return code will indicate an error, and stdout and stderr will
        contain what had already been written to them before the process was
        killed.

  Returns:
    A tuple of stdout, stderr, and retcode from running the provided command.
  """
  logging.debug('Environment variables: %s' % env)

  full_cmd = ' '.join(cmd)
  logging.info('Running: %s', full_cmd)

  shell_value = False
  process = subprocess.Popen(cmd, env=env, shell=shell_value,
                             stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)

  def _KillProcess():
    logging.error('IssueCommand timed out after %d seconds. '
                  'Killing command "%s".', timeout, full_cmd)
    process.kill()

  timer = threading.Timer(timeout, _KillProcess)
  timer.start()

  try:
    stdout, stderr = process.communicate(input)
  finally:
    timer.cancel()

  stdout = stdout.decode('ascii', 'ignore')
  stderr = stderr.decode('ascii', 'ignore')

  debug_text = ('Ran %s. Got return code (%s).\nSTDOUT: %s\nSTDERR: %s' %
                (full_cmd, process.returncode, stdout, stderr))
  if force_info_log or (process.returncode and not suppress_warning):
    logging.info(debug_text)
  else:
    logging.debug(debug_text)

  return stdout, stderr, process.returncode

def ping_job():
  muti_ping_cmd = PKB_PATH + [
    '--benchmark_config_file=express_cnn_hangzhou_us.yaml',
    '--benchmarks=ping',
    '--log_level=info'
  ]
  for _ in range(RETRIES):
    stdout, stderr, retcode = IssueCommand(muti_ping_cmd)
    if retcode != 255:  # Retry on 255 because this indicates an SSH failure
      break
    else:
      success_rate = int(re.findall('^Success rate:\s+(\d+\.?\d*)%\s', stderr.splitlines()[-2]))
      assert success_rate == 100

  result_file_path = re.findall('Publishing \d+ samples to (\/.*\.json)', stderr)
  bq_load_cmd = ['bq',
                 'load',
                 '--source_format=NEWLINE_DELIMITED_JSON',
                 'perfkit_mart.express_cnn_hangzhou_us_opt'] + result_file_path + SCHEMA_PATH
  stdout, _, retcode  = IssueCommand(bq_load_cmd)

if __name__ == "__main__":
  logging.basicConfig(level=logging.INFO)

  for i in range(24*12):
    timestr = (datetime.datetime(2016,01,11,00,00,00)+datetime.timedelta(minutes=i*5)).strftime("%H:%M")
    schedule.every().day.at(timestr).do(ping_job)

  while True:
    schedule.run_pending()
    time.sleep(1)
