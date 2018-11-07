# Flink jobs watchdog 

Tool for monitoring, restart and send notifications to Slack and Email of Flink jobs.
'
## Usage

  watchdog_flink_jobs.py -H flinkHost -p flinkPort -j commaJobList -c launchCommand -r autoRestart -e sendEmail -s sendSlack

Where

  flinkHost -> The domain where flink is hosted.
  flinkPort -> The API port.
  jobList -> Comma separated list of the name of the jobs that you want to monitor.
  launchJobCommand -> Command that is used to execute the job in case it has fallen. We can specify a wildcard, #JOBNAME, so that we can generalize the command to execute jobs with different names. #JOBNAME will be replaced with the name of failed job.
  automatic restart -> Flag that allows you to specify whether you want to not automatically restart the job.
