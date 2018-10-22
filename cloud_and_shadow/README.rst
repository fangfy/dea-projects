
To submit a job to PBS, run ``datacube-stats`` like

.. code-block:: bash

    $ datecube-stats --qsub="project=u46,nodes=1,walltime=10h,mem=medium,queue=normal" --parallel 64 cloud_ls8.yaml

Run on 16 CPU cores, 64 GB memory.
