To calculate standard deviation of the ratings of movies in the ratings dataset. Two mapreduce jobs have been created,
Vasanthi_Mudunuri_Program_4.java contains MapReduce job to calculate the squares of differences between ratings and mean.

Execution Steps:

~]$ hadoop jar Vasanthi_Mudunuri_PA2.jar Vasanthi_Mudunuri_Program_4 /CS5433/PA2/ratings.dat /vmudunu/output4

The output file is given as input to the next mapreduce job.

Vasanthi_Mudunuri_Program_5.java contains MapReduce job to calculate the standard deviation of the ratings of movies.

Execution Steps:

~]$ hadoop jar Vasanthi_Mudunuri_PA2.jar Vasanthi_Mudunuri_Program_5 /CS5433/PA2/output4/part-r-00000 /vmudunu/output5



