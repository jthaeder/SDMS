# tarToHPSS

Scripts to copy picoDsts to HPSS in tar files on a production day basis. A sub-module of the SDMS suite.

### Components
* submitTarJobs.sh    - *Creates the list of 'days' to be backed up and submits jobs*
* tarToHPSS.csh       - *Actual htar called by submitted jobs*
* checkStoredFiles.sh - *Run afterwards to check if all tar files have been created*

#### submitTarJobs.sh
THE script. It creates the list of 'days' to be backed up and submits jobs.
**Important:** *run script as `starofl` user*

##### Select dataSet  
Select the production to be backed up via those parameters within the script:
```BASH
# -- run year
runs="Run14"

# -- subset of production
production="200GeV/physics2/P15ic"
```

##### Important folders:
**Do not change those without need!**
```BASH
# -- Base folder to your installation
basePath=/global/homes/s/starofl/SDMS/tarToHPSS

# --Folder on disk where picoDsts are stored before
projectBaseDir=/project/projectdirs/starprod/picodsts

# -- Folder on HPSS where picoDsts will be backed up to
hpssBaseDir=/nersc/projects/starofl/picodsts
```

##### Output
* All log files of the jobs will be in a `log.<run>` folder.
* `tar_done_Run14.list` and `tar_done_Run14.list.fail` are created and filled by `tarToHPSS.csh` according to success of HTAR operation.

#### checkStoredFiles.sh
After all backup jobs have been finished, run this script to check if all have been successfully backup'ed.

You can check this via: `qstat -u starofl | grep tarToHPSS`

For Selection of dataset and the folders, see above at *submitTarJobs.sh*

#### tarToHPSS.csh
Executes actual `HTAR` command.
Fills `tar_done_Run14.list` and `tar_done_Run14.list.fail` according to success of HTAR operation.

### Installation

###### Requirements
* The package needs to be installed at NERSC.
* Module needs to be run as `starofl` user !!

###### Installation
1. Check that you are `starofl` user
2. Clone from git into `$HOME`
3. Change into `$HOME/SDMS/tarToHPSS`

```BASH
cd $HOME
git clone https://github.com/jthaeder/SDMS.git
cd /SDMS/tarToHPSS
```

###### Workflow
1. Set the proper `runs` and `production` variable in `submitTarJobs.sh` and in `checkStoredFiles.sh`
2. Execute `submitTarJobs.sh`
