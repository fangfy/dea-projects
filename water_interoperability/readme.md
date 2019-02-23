# Water interoperability

Code inherited from https://github.com/opendatacube/datacube-conference-2019. Modified to work at NCI so can be applied to more areas and extended time period.

Noteworthy points:

1. The same WOFS model is used for LS8 and S2. 
2. For LS8, cloud is masked using corresponding pq product. 
3. For S2, L1C data is loaded to create better cloud masks. This step significantly increases the memory usage and processing time. There's an issue running this for 2018 due to missing L1C file locations in database. 
4. For S1, if no suitable threshold is found in the vv+vh distribution, water mask is defined to be not available. This is less aggressive than the original approach but due to the fact that thresholding fails either when there's no/little water or when water is turbulent and gives similar backscatter signal to relatively flat surfaces.
5. Monthly composites are created using S1, S2 and LS8, flagging a pixel as water if more than half of the available observations that month indicate water.
6. Consistency between S1, S2 and LS8 are measured using overlapping (within one day) observations.



