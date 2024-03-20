import subprocess
import argparse
import os

parser = argparse.ArgumentParser()


parser.add_argument("--test", type = int, help="0 prints the command, 1 will execute", default=0)
parser.add_argument("--exp", type = str, help="cxic0415 or cxic0515", default=0)


args = parser.parse_args()


for_real = args.test
exp = args.exp

#%%
que2 = 'psanaq'
#que2 = 'psfehq'


def batchSubmit(cmd, queue=que2, cores=1, log='%j.log', jobName=None, batchType='slurm', params=None):
    """
    Simplify batch jobs submission for lsf & slurm
    :param cmd: command to be executed as batch job
    :param queue: name of batch queue
    :param cores: number of cores
    :param log: log file
    :param batchType: lsf or slurm
    :return: commandline string
    """
    if batchType == "lsf":
        _cmd = "bsub -q " + queue + \
               " -n " + str(cores) + \
               " -o " + log
        if jobName:
            _cmd += " -J " + jobName
        _cmd += cmd
    elif batchType == "slurm":
        _cmd = "sbatch --partition=" + queue + \
               " --output=" + log
        if params is not None:
            for key, value in params.items():
                _cmd += " "+key+"="+str(value)
        else:
            _cmd += " --ntasks=" + str(cores)
        if jobName:
            _cmd += " --job-name=" + jobName
        _cmd += " --wrap=\"" + cmd + "\""
    return _cmd



cmds=[]

# exp = 'cxic0415'
#exp = 'cxic0515'

if exp == 'cxic0415':

 #runs = range(20,21)
 runs = range(17,102)

# runs = range(101,102)
 #runs = range(100,101)
# runs = range(80,81)

 exp = 'cxic0415'

 dirname = f'/reg/d/psdm/cxi/cxic0415/scratch/smarches1/psocake_qoz/{exp}/r'

#dirname = f'/reg/data/ana03/scratch/smarches/cxi/psocake_qoz/{exp}/r'

 indirname = '/reg/d/psdm/cxi/cxic0415/scratch/smarches/psocake/r'
 detname = 'DscCsPad'
 coffset = 0.5886956
 detectorDistance = 0.1386948
 clen = f'{detname}_z'

else:

 exp = 'cxic0515'
 runs = range(9,84)
# runs = range(20,21)

# runs = range(82,84)
 dirname   = f'/reg/d/psdm/cxi/cxic0415/scratch/smarches1/psocake_qoz/{exp}/r'

 indirname = '/reg/d/psdm/cxi/cxic0415/scratch/0515a/cxic0515/smarches/psocake/r'
 #detname = 'DsdCsPad'
 detname = 'CxiDs2.0:Cspad.0'
 coffset = 0.3219988 # 0.3219938
 detectorDistance = 0.147
 clen = 'CXI:DS2:MMS:06.RBV'
 # detname = 'CxiDs2.0:Cspad.0'

spartition = 'psanagpuq'
spartition = 'psanaq'
#optional = '--tag test --noe 100'
optional = ''


for ii in runs:

    # data is saved here:
    directory = f'{dirname}{ii:04}'
    maskfname = f'{indirname}{ii:04}/staticMask.h5'
    if not os.path.isdir(directory):
            os.mkdir(directory)

    #output log is here:
    logf= f'{directory}/../job_{ii:03}.log'
    #logf= f'r{ii}.log'
    jobN = f"{exp[-3:]}_{ii:03}"
    #logf= f'r{pre_transform_method}_{AError}_{ii}.log'

    cmd = f'sbatch --partition={spartition} --ntasks=12 --output={logf} --job-name={jobN} --wrap="mpirun --mca btl ^openib python findPeaksSZ.py -e {exp} -d {detname} --outDir {directory} --algorithm 2 --alg_npix_min 2.0 --alg_npix_max 30.0 --alg_amax_thr 300.0 --alg_atot_thr 600.0 --alg_son_min 10.0 --alg1_thr_low 0.0 --alg1_thr_high 0.0 --alg1_rank 3 --alg1_radius 3 --alg1_dr 2 --psanaMask_on True --psanaMask_calib True --psanaMask_status True --psanaMask_edges True --psanaMask_central True --psanaMask_unbond True --psanaMask_unbondnrs True --mask {maskfname} --clen {clen} --coffset {coffset} --minPeaks 15 --maxPeaks 2048 --minRes -1 --sample sample --instrument CXI --pixelSize 0.00010992 --auto False --detectorDistance {detectorDistance} --access ana -r {ii} --szfile qoz.json {optional}"'
 
## [--tag test --noe 100]"



    #command is this:
   # cmd = f'python producers/smd_producer.py --experiment xcsx1001121 --run {ii} --compressorE {AError} --directory  {directory}  --pretransform {pre_transform_method}'

    # job name (for squeue, limited in size):
    #jobN = f"{exp[-3:]}_{ii:03}"



    print("Submitting batch job: ", cmd, ', job name', jobN)
    #process = subprocess.Popen(cmdb, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    #cmdb = batchSubmit(cmd, log = logf, jobName = jobN)

    #print("batch job command: ", cmdb)
    #print("batch job name: ", jobN)

    if for_real:
     process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
     out, err = process.communicate()
     #cmds.append(cmdb)
     cmds.append(cmd)
     #print(cmdb, ii, cmd)


