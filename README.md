# ArbejdQ

ArbejdQ is a simple background job management / queuing system utilizing Ecto
to persisting jobs and results.
It is not designed to handle a large number of concurrent jobs, but rather handle
a smaller amount of longer running jobs.

A job consists of of the the following:
- Job ID
- Worker module
- Job parameters
- Job status
- Job results
- Queue ID

Both job parameters and job results are stored in Erlang External Term form. The length of
both parameters and result is limit. Large parameter sets and result should be stored in 
some system external to the job processing system.

Jobs are enqueued by using `ArbejdeQ.enqueue/3`. Each job has a designated queue ID, which is
just a string. Each worker node has an execution configuration, which defines how jobs from
the different queues are executed. There is no "global" execution configuration.
