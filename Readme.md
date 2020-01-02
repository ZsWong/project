During training a sample is a timestep of all interested status files of a job which contains all status 
during the last second.
Secretary.py can remove jobs directory

The structure of jobs directory is like below:
- jobs
-- JOBXXXXXXXXXXXXXX(same as the names in zips directory)/
------ WorkSch_TASK.xml
----------- Demodx1/
----------- Demodx2/
---------------- sections/
---------------- status.csv
-------------------- input/
-------------------------------status.csv
-------------------- input_carrierlock/
-------------------- input_carrierlock_bitlock/
-----------samples/
---------------- negtive/
---------------- positive/
------------------------- sections/
-------------------------- input/
------------------------------- samples.npy
-------------------------- input_carrierlock/
-------------------------- input_carrierlock_bitlock/

