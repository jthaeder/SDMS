
## Upload files to XRootD ##

1.) Make list of picoDst files for DataSet
 ./makeInFiles.sh

2.) Preload Files in to loader DB

 ./runInCHOS.sh ./loadListToMongoDB.sh picoDsts_Run14.in 

3.) Start loading jobs

 qsub -o logs -j y -l h_vmem=3G -q mndl_prod.q -t 1-10000 /global/homes/j/jthaeder/SDMS/diskToXRootD/runInCHOS.sh /global/homes/j/jthaeder/SDMS/diskToXRootD/loaderMendel.sh

 # - Some might only add one file - so we start multiple processes





## Connect to MongoDB ##

AS: starxrd@pstarxrdr1

$ mongo pstarxrdr1/star
> use star
> db.loader.find({"load.state" : "unloaded"}).count()
