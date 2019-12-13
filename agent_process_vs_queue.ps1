param (
    [string]$dbServer = $(throw "-dbServer is required")
)




$engines = Invoke-Sqlcmd -Query "select id, substring(serveruri, 8, len(serveruri) - 57) as hostname, servername from engineservers where IsBlocked = 0 order by hostname asc" -Database "cxdb" -ServerInstance $dbServer

$scanrequests = Invoke-Sqlcmd -Query "select ID, SourceID, TaskID, ProjectName, ServerID, CreatedOn, UpdatedOn, Stage, StageDetails from scanrequests where serverid > 0 order by taskid asc;" -Database "cxdb" -ServerInstance $dbServer


$totalInDBOnly = 0
$totalOnEngineOnly = 0
$totalProcInDB = 0
$totalDBWithProc = 0

$taskList = ""
$taskHash = @{};

foreach ( $engine in $engines ) {
   # Write-Host $engine.id "-" $engine.hostname;

    $rhost = $engine.hostname;

    $processes = gwmi -ComputerName "$rhost" -Query "select commandline from win32_process where name='CxEngineAgent.exe'"

    foreach ( $proc in $processes ) {
        
        $taskID = [regex]::match( $proc.CommandLine, '.*.exe" (\d+)_.*' ).Groups[1].Value
        $sourceID = [regex]::match( $proc.CommandLine, '.*.exe" \d+_(.{36})' ).Groups[1].Value
        #Write-Host $proc.Commandline " -> $taskID, $sourceID"

        $scanInDB = 0
        foreach ( $scan in $scanrequests ) {
            if ( $scan.SourceID -eq $sourceID -and $scan.taskID -eq $taskID -and $scan.ServerID -eq $engine.id ) {
                # match 
                $scanInDB = 1
            }
        }

        if ( $scanInDB -eq 0 ) {
            #Write-Host "$taskID`t$sourceID`t$($engine.ServerName) ($($engine.hostname))`tRunning, Not in DB"
            #Write-Host "CxEngineAgent.exe is running on " $engine.ServerName " (" $engine.hostname ") for project#$taskID (sourceID: $sourceID) but it is not in the database."
            #$totalOnEngineOnly++
            if ( $taskID -match "^\d+$" ) { 
                $task = @{};
                $task['ID'] = $taskID;
                $task['sourceID'] = $sourceID;
                $task['engineID'] = $engine.id;
                $task['msg'] = "$taskID`t$sourceID`t$($engine.ServerName) ($($engine.hostname))`tRunning, Not in DB";

                $taskHash.add( $taskID, $task );
                if ( $taskList -eq "" ) {
                    $taskList = "$([convert]::ToInt32($taskID, 10))";
                } else {
                    $taskList = "$taskList, $([convert]::ToInt32($taskID, 10))";
                }
            } else {
                Write-Host "Error, received invalid taskID: $taskID"
            }
        } else {
            $totalProcInDB++;
        }
    }

    foreach ( $scan in $scanrequests ) {
        #Write-Host "`tscan: $($scan.taskID), $($scan.ProjectName)"
        $scanOnEngine = 0
        if ($scan.ServerID -eq $engine.id ) {
            foreach ( $proc in $processes ) {
                $taskID = [regex]::match( $proc.CommandLine, '.*.exe" (\d+)_.*' ).Groups[1].Value
                $sourceID = [regex]::match( $proc.CommandLine, '.*.exe" \d+_(.{36})' ).Groups[1].Value

                if ( $scan.SourceID -eq $sourceID -and $scan.taskID -eq $taskID -and $scan.ServerID -eq $engine.id ) {
                    # match 
                    $scanOnEngine = 1
                }
            }
            if ( $scanOnEngine -eq 0 ) {
                #Write-Host "DB says scan#" $scan.ID " of project#" $scan.taskID " (" $scan.ProjectName ") with SourceID " $scan.SourceID " is running on " $engine.ServerName " (" $engine.hostname "), but it's not."
                #Write-Host " - > " $scan.Stage ": " $scan.StageDetails

                if ( $scan.Stage -eq 7 -and $scan.StageDetails -match "Scan completed" ) {
                    # finishing up, so process is gone but request still in progress
                } elseif ( $scan.Stage -le 3 ) {
                    # starting up, may not be running yet.
                } else {
                    Write-Host "$($scan.taskID)`t$($scan.SourceID)`t$($engine.ServerName) ($($engine.hostname))`tNot running, In DB`tStage: $($scan.Stage)`tCreated: $($scan.CreatedOn)`tUpdated: $($scan.UpdatedOn)`t$($scan.ProjectName) - Stage $($scan.Stage): $($scan.StageDetails.SubString(0,[math]::min($scan.StageDetails.Length,20)))"
                    $totalInDBOnly++
                }
            } else {
                $totalDBWithProc++;
            }
        }
        

    }
    #return;
}

#Write-Host "Leftover tasks: $taskList"
if ( $taskList -ne "" ) {
    $completedTasks = Invoke-Sqlcmd -Query "select taskid, sourceid, serverid, FinishTime from taskscans where taskid in ($taskList) and finishtime > dateadd( minute, -30, getdate());" -Database "cxdb" -ServerInstance $dbServer

    #$completedTasks;

    foreach ( $taskID in $taskHash.Keys ) {
        $recentlyFinished = 0
        $task = $taskHash[$taskID];


        #Write-Host "Checking $($task['id']) - $($task['sourceID']) - $($task['engineID'])"

        foreach ( $finID in $completedtasks ) {
            #$finID;
            if ( $finID.taskid -eq $task['id'] -and $finID.sourceid -eq $task['sourceID'] -and $finID.engineid -eq $task['engineID'] ) {
                $recentlyFinished = 1
            }
        }

        if ( $recentlyFinished -eq 0 ) {
            Write-Host $task['msg']
            $totalOnEngineonly ++;
        }
    }
}

Write-Host "Good: $totalProcInDB processes running have a db entry"
if ( $totalProcInDB -ne $totalDBWithProc ) {
    Write-Host "Bad: ... but somehow $totalDBWithProc db entries have a matching process?"
}
Write-Host "Bad: $totalInDBOnly listed in DB but not on engines."
Write-Host "Bad: $totalOnEngineOnly running on engines but not listed in DB."