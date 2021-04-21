@echo off

:start
cls 
echo You are about to install python packages required for the AD-LS
echo ============================================================================


set /p x=Are you sure?(y/n)
IF '%x%' == 'y' GOTO NUM_1
IF '%x%' == 'Y' GOTO NUM_1
IF '%x%' == 'n' GOTO NUM_2
IF '%x%' == 'N' GOTO NUM_2
GOTO start


:NUM_1
echo Installing all packages..
python -m pip install -r requirements.txt
echo All required files for the AD-LS have been installed successfully
GOTO NUM_2


:NUM_2
pause
exit

