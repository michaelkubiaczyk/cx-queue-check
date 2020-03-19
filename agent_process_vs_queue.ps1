param (
    [string]$dbServer = $(throw "-dbServer is required")
)




$engines = Invoke-Sqlcmd -Query "select id, substring(serveruri, 8, len(serveruri) - 57) as hostname, servername from engineservers where IsBlocked = 0 order by hostname asc" -Database "cxdb" -ServerInstance $dbServer
$scanrequests = Invoke-Sqlcmd -Query "select ID, SourceID, TaskID, ProjectName, ServerID, CreatedOn, UpdatedOn, Stage, StageDetails from scanrequests where serverid > 0 order by taskid asc;" -Database "cxdb" -ServerInstance $dbServer

# read previous file
$lastDBState = get-content -path "lastDBState.txt"
$lastDBCount = $lastDBState.Length
$lastDBTimeStamp = $lastDBState[0]
if ( $lastDBState -is [string] ) {
    $lastDBTimeStamp = $lastDBState
    $lastDBCount = 0
}

$timeSinceLastDBState = new-TimeSpan -start $lastDBTimeStamp -end (Get-Date)
Write-Host "Time since last DB state check:" $timeSinceLastDBState.minutes " min"

$currentStartTime = (Get-Date)
Set-Content -Path "lastProcessState.txt" -value $currentStartTime
Set-Content -Path "lastDBState.txt" -value $currentStartTime


$totalProcCount = 0
$totalDBCount = $scanrequests.length
$totalInDBOnly = 0
$totalOnEngineOnly = 0
$totalProcInDB = 0
$totalDBWithProc = 0

$taskList = ""
$procHash = @{};
$procCount = 0;


foreach ( $engine in $engines ) {
   # Write-Host $engine.id "-" $engine.hostname;

    $rhost = $engine.hostname;

    $processes = gwmi -ComputerName "$rhost" -Query "select commandline from win32_process where name='CxEngineAgent.exe'"

    foreach ( $proc in $processes ) {
        $totalProcCount++;
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

                $procCount++;

                $procHash.add( $procCount, $task );
                if ( $taskList -eq "" ) {
                    $taskList = "$([convert]::ToInt32($taskID, 10))";
                } else {
                    $taskList = "$taskList, $([convert]::ToInt32($taskID, 10))";
                }
            } else {
                Write-Host "Error, received invalid taskID $taskID for process" $proc.CommandLine
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
                $state = "new";
                $toFile = "$($scan.taskID),$($scan.SourceID),$($scan.ServerID)"
           
                for ( $i = 1; $i -lt $lastDBCount; $i++ ) {
                    $e = $lastDBState[$i].split(";");
                    if ( $e[1] -eq $toFile ) {
                        $state = "$((new-Timespan -Start $e[0] -End $currentStartTime).Minutes) min"
                        Add-Content -Path "lastDBState.txt" -Value $lastDBState[$i]
                    }
                }
                if ( $state -eq "new" ) {
                    Add-Content -Path "lastDBState.txt" -Value "$currentStartTime;$toFile";
                }

                Write-Host "Stuck ($state);" $scan.ID ";" $scan.taskID ";" $scan.ProjectName ";" $scan.SourceID ";" $engine.ServerName " (" $engine.hostname ")"                
                #Write-Host "$($scan.taskID)`t$($scan.SourceID)`t$($engine.ServerName) ($($engine.hostname))`tNot running, In DB`tStage: $($scan.Stage)`tCreated: $($scan.CreatedOn)`tUpdated: $($scan.UpdatedOn)`t$($scan.ProjectName) - Stage $($scan.Stage): $($scan.StageDetails.SubString(0,[math]::min($scan.StageDetails.Length,20)))"

                $totalInDBOnly++
            } else {
                $totalDBWithProc++;
            }
        }
        

    }
    #return;
}

Write-Host "There are $totalDBCount scans running in the database, and $totalProcCount processes running on engines"

Write-Host "Leftover tasks: $taskList"
if ( $taskList -ne "" ) {
    Write-Host "Executing select taskid, sourceid, serverid, FinishTime from taskscans where taskid in ($taskList) and finishtime > dateadd( minute, -30, getdate());"
    $completedTasks = Invoke-Sqlcmd -Query "select taskid, sourceid, serverid, FinishTime from taskscans where taskid in ($taskList) and finishtime > dateadd( minute, -30, getdate());" -Database "cxdb" -ServerInstance $dbServer

    #$completedTasks;

    foreach ( $taskID in $procHash.Keys ) {
        $recentlyFinished = 0
        $task = $procHash[$taskID];

        Add-Content -Path "lastProcessState.txt" -value "$($task['id']),$($task['sourceID']), $($task['engineID'])";

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

Write-Host "[Good] $totalProcInDB processes are running and have a matching DB entry"
if ( $totalProcInDB -ne $totalDBWithProc ) {
    Write-Host "[Bad] ... but somehow $totalDBWithProc DB entries have a matching process? (numbers should match)"
}

if ( $totalInDBOnly -gt 0 ) {
    Write-Host "[Bad] $totalInDBOnly listed in DB but not on engines."
}

if ( $totalOnEngineOnly -gt 0 ) {
    Write-Host "[Bad] $totalOnEngineOnly running on engines but not listed in DB."
}