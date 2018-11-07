#!/usr/bin/python
import requests
import json
import sys,getopt
import subprocess
import datetime

def main(argv):
 
    # Configuration variables
    flinkHost=''
    flinkPort=''
    watchingJobList=''
    launchJobCommand=''
    automaticRestart=False
    sendEmail=False
    emailConfig=''
    sendSlack=False
    slackConfig=''

    # Argv parsing
    try:
        opts,args = getopt.getopt(argv,"hH:p:j:c:res",["help","flinkHost=","flinkPort=","jobList=","launchJobCommand=","autoRestart","sendEmail","sendSlack"])
    except getopt.GetoptError:
        print 'Usage:\nwatchdog_flink_jobs.py -H <flinkHost> -p <flinkPort> -j <commaJobList> -c <launchCommand> -r <autoRestart> -e <sendEmail> -s <sendSlack>\n'
        sys.exit(2) 
    for opt,arg in opts:
        if opt in ("-h","--help"):
            print 'Usage:\nwatchdog_flink_jobs.py -H <flinkHost> -p <flinkPort> -j <commaJobList> -c <launchCommand> -r <autoRestart> -e <emailConfig> -s <slackConfig>\n'
            sys.exit()
        elif opt in ("-H","--flinkHost"):
            flinkHost=arg
        elif opt in ("-p","--flinkPort"):
            flinkPort=arg
        elif opt in ("-j","--jobList"):
            watchingJobList=arg.split(',')
        elif opt in ("-c","--flinkCommand"):
            launchJobCommand=arg
        elif opt in ("-r","--autoRestart"):
            automaticRestart=True
        elif opt in ("-e","--sendEmail"):
            sendEmail=True
            emailConfig=arg
        elif opt in ("-s","--sendSlack"):
            sendSlack=True
            slackConfig=arg
        
    checkAlarm(flinkHost, flinkPort, watchingJobList, launchJobCommand, automaticRestart, sendEmail, emailConfig, sendSlack, slackConfig)

def checkAlarm(flinkHost, flinkPort, watchingJobList, launchJobCommand, automaticRestart, sendEmail, emailConfig, sendSlack, slackConfig):
    # Starting watchingJobs check
    print str(datetime.datetime.now()) + ' Ejecutando watchdog para flink en: ' + flinkHost + ':' + flinkPort

    # Get running jobs from flink
    runningJobList = getFlinkRunningJobList(flinkHost,flinkPort)

    # Check if the watchingJobs are running
    for watchingJob in watchingJobList:
        if isRunning(watchingJob, runningJobList) != True:
            alert = {}
            alert['failedJob'] = watchingJob
            alert['error'] = getException(flinkHost, flinkPort, watchingJob)
            
            # Restart job if automaticStart set to True
            if automaticRestart:
                runJob(watchingJob, launchJobCommand)
            
            # Send email if sendEmail set to True
            if sendEmail:
                sendEmail(emailConfig, alert)
            
            # Send slack if sendSlack set to True
            if sendSlack:
                sendSlack(slackConfig, alert)

def isRunning(watchingJob, runningJobList):
    
    for runningJob in runningJobList:
        if watchingJob == runningJob['name']:
            print str(datetime.datetime.now()) + ' INFO: ' + 'Job ' + watchingJob + ' is working!'
            return True
    
    print str(datetime.datetime.now()) + ' ERROR: ' + 'Job ' + watchingJob + ' has failed.'
    return False

def runJob(jobName, launchJobCommand):
 
    print str(datetime.datetime.now()) + 'Restarting ' + jobName + ' job...'
    launchJobNameCommand = launchJobCommand.replace('#JOBNAME',jobName)
    print str(datetime.datetime.now()) + 'Launching command ' + launchJobNameCommand
    #result = subprocess.run(launchJobNameCommand, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    #print result.stdout

def getFlinkRunningJobList(flinkHost,flinkPort):
 
    runningJobsOverviewLink= 'http://' + flinkHost + ':' + flinkPort +'/joboverview/running'
    return requests.get(runningJobsOverviewLink).json()['jobs']

def getFlinkFinishedJobList(flinkHost, flinkPort):
    
    finishedJobsOverviewLink= 'http://' + flinkHost + ':' + flinkPort +'/joboverview/completed'
    return requests.get(finishedJobsOverviewLink).json()['jobs']

def getFlinkJobException(flinkHost, flinkPort, jid):
    
    finishedJobsOverviewLink= 'http://' + flinkHost + ':' + flinkPort +'/jobs/' + jid + '/exceptions'
    return requests.get(finishedJobsOverviewLink).json()

def getException(flinkHost, flinkPort, failedJobName):

    lastFinishedJobTS = 0
    jobid = 0
    finishedJobList = getFlinkFinishedJobList(flinkHost, flinkPort)
    for finishedJob in finishedJobList:
        if finishedJob['name'] == failedJobName:
            if finishedJob['start-time'] > lastFinishedJobTS:
                jobid=finishedJob['jid']
                lastFinishedJobTS=finishedJob['start-time']
    if jobid != 0:
        exception = getFlinkJobException(flinkHost,flinkPort,jobid)
        if exception == '':
            exception = 'Stopped without exception.'
    else:
        exception = "No se han encontrado excepciones para el job"
    print exception
    return exception

def sendEmail(config, alert):
    
    emailConfig=json.loads(config)
    
    header  = 'From: %s' % emailConfig['from_addr']
    header += 'To: %s' % ','.join(emailConfig['to_addr_list'])
    header += 'Cc: %s' % ','.join(emailConfig['cc_addr_list'])
    header += 'Subject: %s' % emailConfig['subject']
    
    message = header + str(alert)
    
    server = smtplib.SMTP(emailConfig['smtpserver'])
    server.starttls()
    server.login(emailConfig['login'],emailConfig['password'])
    problems = server.sendmail(emailConfig['from_addr'], emailConfig['to_addr_list'], message)
    server.quit()
    print datetime.datetime.now() + 'Succesfully sended email'

def sendSlack(config, alert):
    # Set the webhook_url to the one provided by Slack when you create the webhook at https://my.slack.com/services/new/incoming-webhook/
    slackConfig = json.loads(config)
    webhook_url =  slackConfig['webhookUrl']
    slack_data = alert

    response = requests.post(webhook_url, data=json.dumps(slack_data), headers={'Content-Type': 'application/json'})
 
    if response.status_code != 200:
        raise ValueError('Request to slack returned an error %s, the response is:\n%s' % (response.status_code, response.text))
    else:
        print datetime.datetime.now() + 'Succesfully sended slack'

if __name__ == "__main__":
    main(sys.argv[1:])
