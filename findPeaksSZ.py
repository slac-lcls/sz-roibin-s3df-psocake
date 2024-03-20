# Find Bragg peaks
from peakFinderMaster import runmaster
from peakFinderClientAutoSZ import runclient as runclientAuto
from utils import *
import h5py
import glob
import numpy as np
from mpi4py import MPI
import os

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()
assert size>1, 'At least 2 MPI ranks required'
numClients = size-1

import argparse
parser = argparse.ArgumentParser()
parser.add_argument('-e','--exp', help="experiment name (e.g. cxic0415)", type=str)
parser.add_argument('-r','--run', help="run number (e.g. 24)", type=int)
parser.add_argument('-d','--det', help="detector name (e.g. pnccdFront)", type=str)
parser.add_argument('-o','--outDir', help="output directory where .cxi will be saved (e.g. /sdf/data/lcls/ds/cxi/cxic0415/scratch)", type=str)
parser.add_argument("-p","--imageProperty",help="determines what preprocessing is done on the image",default=1, type=int)
parser.add_argument("--algorithm",help="number of events to process",default=1, type=int)
parser.add_argument("--alg_npix_min",help="number of events to process",default=1., type=float)
parser.add_argument("--alg_npix_max",help="number of events to process",default=45., type=float)
parser.add_argument("--alg_amax_thr",help="number of events to process",default=250., type=float)
parser.add_argument("--alg_atot_thr",help="number of events to process",default=330., type=float)
parser.add_argument("--alg_son_min",help="number of events to process",default=10., type=float)
parser.add_argument("--alg1_thr_low",help="number of events to process",default=80., type=float)
parser.add_argument("--alg1_thr_high",help="number of events to process",default=270., type=float)
parser.add_argument("--alg1_rank",help="number of events to process",default=3, type=int)
parser.add_argument("--alg1_radius",help="number of events to process",default=3, type=int)
parser.add_argument("--alg1_dr",help="number of events to process",default=1., type=float)
parser.add_argument("--streakMask_on",help="streak mask on",default="False", type=str)
parser.add_argument("--streakMask_sigma",help="streak mask sigma above background",default=0., type=float)
parser.add_argument("--streakMask_width",help="streak mask width",default=0, type=float)
parser.add_argument("--userMask_path",help="full path to user mask numpy array",default=None, type=str)
parser.add_argument("--psanaMask_on",help="psana mask on",default="False", type=str)
parser.add_argument("--psanaMask_calib",help="psana calib on",default="False", type=str)
parser.add_argument("--psanaMask_status",help="psana status on",default="False", type=str)
parser.add_argument("--psanaMask_edges",help="psana edges on",default="False", type=str)
parser.add_argument("--psanaMask_central",help="psana central on",default="False", type=str)
parser.add_argument("--psanaMask_unbond",help="psana unbonded pixels on",default="False", type=str)
parser.add_argument("--psanaMask_unbondnrs",help="psana unbonded pixel neighbors on",default="False", type=str)
parser.add_argument("--mask",help="static mask",default='', type=str)
parser.add_argument("-n","--noe",help="number of events to process",default=-1, type=int)
parser.add_argument("--medianBackground",help="subtract median background",default=0, type=int)
parser.add_argument("--medianRank",help="median background window size",default=0, type=int)
parser.add_argument("--radialBackground",help="subtract radial background",default=0, type=int)
parser.add_argument("--sample",help="sample name (e.g. lysozyme)",default='', type=str)
parser.add_argument("--instrument",help="instrument name (e.g. CXI)", default=None, type=str)
parser.add_argument("--clen", help="camera length epics name (e.g. CXI:DS1:MMS:06.RBV or CXI:DS2:MMS:06.RBV)", type=str)
parser.add_argument("--coffset", help="camera offset, CXI home position to sample (m)", default=0, type=float)
parser.add_argument("--detectorDistance", help="detector distance from interaction point (m)", default=0, type=float)
parser.add_argument("--pixelSize",help="pixel size",default=0, type=float)
parser.add_argument("--minPeaks", help="Index only if above minimum number of peaks",default=15, type=int)
parser.add_argument("--maxPeaks", help="Index only if below maximum number of peaks",default=2048, type=int)
parser.add_argument("--minRes", help="Index only if above minimum resolution",default=0, type=int)
parser.add_argument("--localCalib", help="Use local calib directory. A calib directory must exist in your current working directory.", action='store_true')
parser.add_argument("--profile", help="Turn on profiling. Saves timing information for calibration, peak finding, and saving to hdf5", action='store_true')
parser.add_argument("--cxiVersion", help="cxi version",default=140, type=int)
parser.add_argument("--auto", help="automatically determine peak finding parameter per event", default="False", type=str)
# LCLS specific
parser.add_argument("-a","--access", help="Set data node access: {ana,ffb}",default="ana", type=str)
parser.add_argument("-t","--tag", help="Set tag for cxi filename",default="", type=str)
parser.add_argument("-i","--inputImages", default="", type=str, help="full path to hdf5 file with calibrated CsPad images saved as /data/data and /eventNumber. It can be in a cheetah format (3D) or psana unassembled format (4D)")
# PAL specific
parser.add_argument("--dir", help="PAL directory where the detector images (hdf5) are stored", default=None, type=str)
parser.add_argument("--currentRun", help="current run number", type=int)

# Roibin-SZ specific
parser.add_argument("--szfile", help="sz json file", default="sz.json", type=str)

args = parser.parse_args()


if 'LCLS' in os.environ['PSOCAKE_FACILITY'].upper():
    facility = 'LCLS'
    import psanaWhisperer, psana
elif 'PAL' in os.environ['PSOCAKE_FACILITY'].upper():
    facility = 'PAL'

def getNoe(args):
    if facility == "LCLS":
        runStr = "%04d" % args.run
        access = "exp="+args.exp+":run="+runStr+':idx'
        if 'ffb' in args.access.lower(): access += ':dir=/sdf/data/lcls/ds/' + args.exp[:3] + '/' + args.exp + '/xtc'
        print("findPeaks: ", access)
        ds = psana.DataSource(access)
        run = next(ds.runs())
        times = run.times()
        # check if the user requested specific number of events
        if args.noe == -1:
            numJobs = len(times)
        else:
            if args.noe <= len(times):
                numJobs = args.noe
            else:
                numJobs = len(times)
    elif facility == "PAL":
        # check if the user requested specific number of events
        if args.noe == -1:
            numJobs = numEvents
        else:
            if args.noe <= numEvents:
                numJobs = args.noe
            else:
                numJobs = numEvents
    return numJobs

if args.localCalib: psana.setOption('psana.calib-dir','./calib')

if rank == 0:
    if facility == 'LCLS':
        # Set up psana
        ps = psanaWhisperer.psanaWhisperer(args.exp, args.run, args.det, args.clen, args.localCalib, access=args.access)
        ps.setupExperiment()
        numEvents = ps.eventTotal
        img = None
        for i in np.arange(numEvents):
            ps.getEvent(i)
            img = ps.getCheetahImg()
            if img is not None:
                print("Found an event with image: ", i)
                break
    elif facility == 'PAL':
        temp = args.dir + '/' + args.exp[:3] + '/' + args.exp + \
               '/data/r' + str(args.run).zfill(4) + '/*.h5'
        _files = glob.glob(temp)
        f = h5py.File(_files[0], 'r')
        img = f['/data'][()]
        f.close()
        numEvents = len(_files)
    (dim0, dim1) = img.shape

    runStr = "%04d" % args.run
    fname = args.outDir + '/' + args.exp +"_"+ runStr
    if not os.path.exists(args.outDir):
        os.makedirs(args.outDir)


    if args.tag: fname += '_' + args.tag
    fname += ".cxi"
    # Get number of events to process
    numJobs = getNoe(args)

    # Create hdf5 and save psana input
    myHdf5 = h5py.File(fname, 'w')
    myHdf5['/status/findPeaks'] = 'fail'
    # Save user input arguments
    dt = h5py.special_dtype(vlen=bytes)
    myInput = ""
    for key,value in vars(args).items():
        myInput += key
        myInput += " "
        myInput += str(value)
        myInput += "\n"
    dset = myHdf5.create_dataset("/psocake/input",(1,), dtype=dt)
    dset[...] = myInput
    myHdf5.flush()

    myHdf5.create_dataset("cxi_version", data=args.cxiVersion)
    myHdf5.flush()

    dt = h5py.special_dtype(vlen=float)
    dti = h5py.special_dtype(vlen=np.dtype('int32'))

    if facility == 'LCLS':
        ###################
        # LCLS
        ###################
        lcls_1 = myHdf5.create_group("LCLS")
        lcls_detector_1 = lcls_1.create_group("detector_1")
        ds_lclsDet_1 = lcls_detector_1.create_dataset("EncoderValue",(0,),
                                                      maxshape=(None,),
                                                      dtype=float)
        ds_lclsDet_1.attrs["axes"] = "experiment_identifier"

        ds_ebeamCharge_1 = lcls_detector_1.create_dataset("electronBeamEnergy",(0,),
                                                          maxshape=(None,),
                                                          dtype=float)
        ds_ebeamCharge_1.attrs["axes"] = "experiment_identifier"

        ds_beamRepRate_1 = lcls_detector_1.create_dataset("beamRepRate",(0,),
                                                          maxshape=(None,),
                                                          dtype=float)
        ds_beamRepRate_1.attrs["axes"] = "experiment_identifier"

        ds_particleN_electrons_1 = lcls_detector_1.create_dataset("particleN_electrons",(0,),
                                                                  maxshape=(None,),
                                                                  dtype=float)
        ds_particleN_electrons_1.attrs["axes"] = "experiment_identifier"

        ds_eVernier_1 = lcls_1.create_dataset("eVernier",(0,),
                                              maxshape=(None,),
                                              dtype=float)
        ds_eVernier_1.attrs["axes"] = "experiment_identifier"

        ds_charge_1 = lcls_1.create_dataset("charge",(0,),
                                            maxshape=(None,),
                                            dtype=float)
        ds_charge_1.attrs["axes"] = "experiment_identifier"

        ds_peakCurrentAfterSecondBunchCompressor_1 = lcls_1.create_dataset("peakCurrentAfterSecondBunchCompressor",(0,),
                                                                           maxshape=(None,),
                                                                           dtype=float)
        ds_peakCurrentAfterSecondBunchCompressor_1.attrs["axes"] = "experiment_identifier"

        ds_pulseLength_1 = lcls_1.create_dataset("pulseLength",(0,),
                                                 maxshape=(None,),
                                                 dtype=float)
        ds_pulseLength_1.attrs["axes"] = "experiment_identifier"

        ds_ebeamEnergyLossConvertedToPhoton_mJ_1 = lcls_1.create_dataset("ebeamEnergyLossConvertedToPhoton_mJ",(0,),
                                                                         maxshape=(None,),
                                                                         dtype=float)
        ds_ebeamEnergyLossConvertedToPhoton_mJ_1.attrs["axes"] = "experiment_identifier"

        ds_calculatedNumberOfPhotons_1 = lcls_1.create_dataset("calculatedNumberOfPhotons",(0,),
                                                               maxshape=(None,),
                                                               dtype=float)
        ds_calculatedNumberOfPhotons_1.attrs["axes"] = "experiment_identifier"

        ds_photonBeamEnergy_1 = lcls_1.create_dataset("photonBeamEnergy",(0,),
                                                      maxshape=(None,),
                                                      dtype=float)
        ds_photonBeamEnergy_1.attrs["axes"] = "experiment_identifier"

        ds_wavelength_1 = lcls_1.create_dataset("wavelength",(0,),
                                                maxshape=(None,),
                                                dtype=float)
        ds_wavelength_1.attrs["axes"] = "experiment_identifier"

        ds_sec_1 = lcls_1.create_dataset("machineTime",(0,),
                                         maxshape=(None,),
                                         dtype=int)
        ds_sec_1.attrs["axes"] = "experiment_identifier"

        ds_nsec_1 = lcls_1.create_dataset("machineTimeNanoSeconds",(0,),
                                          maxshape=(None,),
                                          dtype=int)
        ds_nsec_1.attrs["axes"] = "experiment_identifier"

        ds_fid_1 = lcls_1.create_dataset("fiducial",(0,),
                                         maxshape=(None,),
                                         dtype=int)
        ds_fid_1.attrs["axes"] = "experiment_identifier"

        ds_photonEnergy_1 = lcls_1.create_dataset("photon_energy_eV",(0,),
                                                  maxshape=(None,),
                                                  dtype=float) # photon energy in eV
        ds_photonEnergy_1.attrs["axes"] = "experiment_identifier"

        ds_wavelengthA_1 = lcls_1.create_dataset("photon_wavelength_A",(0,),
                                                 maxshape=(None,),
                                                 dtype=float)
        ds_wavelengthA_1.attrs["axes"] = "experiment_identifier"

        #### Datasets not in Cheetah ###
        ds_evtNum_1 = lcls_1.create_dataset("eventNumber",(0,),
                                            maxshape=(None,),
                                            dtype=int)
        ds_evtNum_1.attrs["axes"] = "experiment_identifier"

        ds_evr0_1 = lcls_detector_1.create_dataset("evr0",(0,),
                                                   maxshape=(None,),
                                                   dtype=dti)
        ds_evr0_1.attrs["axes"] = "experiment_identifier"

        ds_evr1_1 = lcls_detector_1.create_dataset("evr1",(0,),
                                                   maxshape=(None,),
                                                   dtype=dti)
        ds_evr1_1.attrs["axes"] = "experiment_identifier"

        ds_ttspecAmpl_1 = lcls_1.create_dataset("ttspecAmpl",(0,),
                                                 maxshape=(None,),
                                                 dtype=float)
        ds_ttspecAmpl_1.attrs["axes"] = "experiment_identifier"

        ds_ttspecAmplNxt_1 = lcls_1.create_dataset("ttspecAmplNxt",(0,),
                                                   maxshape=(None,),
                                                   dtype=float)
        ds_ttspecAmplNxt_1.attrs["axes"] = "experiment_identifier"

        ds_ttspecFltpos_1 = lcls_1.create_dataset("ttspecFltPos",(0,),
                                                  maxshape=(None,),
                                                  dtype=float)
        ds_ttspecFltpos_1.attrs["axes"] = "experiment_identifier"

        ds_ttspecFltposFwhm_1 = lcls_1.create_dataset("ttspecFltPosFwhm",(0,),
                                                      maxshape=(None,),
                                                      dtype=float)
        ds_ttspecFltposFwhm_1.attrs["axes"] = "experiment_identifier"

        ds_ttspecFltposPs_1 = lcls_1.create_dataset("ttspecFltPosPs",(0,),
                                                    maxshape=(None,),
                                                    dtype=float)
        ds_ttspecFltposPs_1.attrs["axes"] = "experiment_identifier"

        ds_ttspecRefAmpl_1 = lcls_1.create_dataset("ttspecRefAmpl",(0,),
                                                   maxshape=(None,),
                                                   dtype=float)
        ds_ttspecRefAmpl_1.attrs["axes"] = "experiment_identifier"

        lcls_injector_1 = lcls_1.create_group("injector_1")
        ds_pressure_1 = lcls_injector_1.create_dataset("pressureSDS",(0,),
                                                       maxshape=(None,),
                                                       dtype=float)
        ds_pressure_1.attrs["axes"] = "experiment_identifier"
        ds_pressure_2 = lcls_injector_1.create_dataset("pressureSDSB",(0,),
                                                       maxshape=(None,),
                                                       dtype=float)
        ds_pressure_2.attrs["axes"] = "experiment_identifier"

        myHdf5.flush()
    elif facility == 'PAL':
        ###################
        # PAL
        ###################
        pal_1 = myHdf5.create_group("PAL")

        #### Datasets not in Cheetah ###
        ds_evtNum_1 = pal_1.create_dataset("eventNumber", (0,),
                                            maxshape=(None,),
                                            dtype=int)
        ds_evtNum_1.attrs["axes"] = "experiment_identifier"
        ds_photonEnergy_1 = pal_1.create_dataset("photon_energy_eV",(0,),
                                                 maxshape=(None,),
                                                 dtype=float) # photon energy in eV
        ds_photonEnergy_1.attrs["axes"] = "experiment_identifier"
        myHdf5.flush()

    ###################
    # entry_1
    ###################
    if facility == 'LCLS':
        entry_1 = myHdf5.create_group("entry_1")
        ds_expId = entry_1.create_dataset("experimental_identifier",(0,),
                                                 maxshape=(None,),
                                                 dtype=int)
        ds_expId.attrs["axes"] = "experiment_identifier"

        myHdf5.create_dataset("/entry_1/result_1/nPeaksAll", data=np.ones(numJobs,)*-1, dtype=int)
        myHdf5.create_dataset("/entry_1/result_1/peakXPosRawAll", (numJobs,args.maxPeaks), dtype=float, chunks=(1,args.maxPeaks))
        myHdf5.create_dataset("/entry_1/result_1/peakYPosRawAll", (numJobs,args.maxPeaks), dtype=float, chunks=(1,args.maxPeaks))
        myHdf5.create_dataset("/entry_1/result_1/peakTotalIntensityAll", (numJobs,args.maxPeaks), dtype=float, chunks=(1,args.maxPeaks))
        myHdf5.create_dataset("/entry_1/result_1/peakMaxIntensityAll", (numJobs,args.maxPeaks), dtype=float, chunks=(1,args.maxPeaks))
        myHdf5.create_dataset("/entry_1/result_1/peakRadiusAll", (numJobs,args.maxPeaks), dtype=float, chunks=(1,args.maxPeaks))
        # PeakNet labels
        myHdf5.create_dataset("/entry_1/result_1/centreRowAll", (numJobs,args.maxPeaks), dtype=float, chunks=(1,args.maxPeaks))
        myHdf5.create_dataset("/entry_1/result_1/centreColAll", (numJobs,args.maxPeaks), dtype=float, chunks=(1,args.maxPeaks))
        myHdf5.create_dataset("/entry_1/result_1/minPeakRowAll", (numJobs,args.maxPeaks), dtype=float, chunks=(1,args.maxPeaks))
        myHdf5.create_dataset("/entry_1/result_1/maxPeakRowAll", (numJobs,args.maxPeaks), dtype=float, chunks=(1,args.maxPeaks))
        myHdf5.create_dataset("/entry_1/result_1/minPeakColAll", (numJobs,args.maxPeaks), dtype=float, chunks=(1,args.maxPeaks))
        myHdf5.create_dataset("/entry_1/result_1/maxPeakColAll", (numJobs,args.maxPeaks), dtype=float, chunks=(1,args.maxPeaks))

        myHdf5.create_dataset("/entry_1/result_1/maxResAll", data=np.ones(numJobs,)*-1, dtype=int)
        myHdf5.create_dataset("/entry_1/result_1/likelihoodAll", data=np.ones(numJobs, ) * -1, dtype=float)

        myHdf5.create_dataset("/entry_1/result_1/timeToolDelayAll", data=np.ones(numJobs, ) * -1, dtype=float)
        myHdf5.create_dataset("/entry_1/result_1/laserTimeZeroAll", data=np.ones(numJobs, ) * -1, dtype=float)
        myHdf5.create_dataset("/entry_1/result_1/laserTimeDelayAll", data=np.ones(numJobs, ) * -1, dtype=float)
        myHdf5.create_dataset("/entry_1/result_1/laserTimePhaseLockedAll", data=np.ones(numJobs, ) * -1, dtype=float)
        myHdf5.flush()

        if args.profile:
            myHdf5.create_dataset("/entry_1/result_1/calibTime", data=np.zeros(numJobs, ), dtype=float)
            myHdf5.create_dataset("/entry_1/result_1/peakTime", data=np.zeros(numJobs, ), dtype=float)
            myHdf5.create_dataset("/entry_1/result_1/saveTime", data=np.zeros(numJobs, ), dtype=float)
            myHdf5.create_dataset("/entry_1/result_1/reshapeTime", (0,), maxshape=(None,), dtype=float)
            myHdf5.create_dataset("/entry_1/result_1/totalTime", data=np.zeros(numJobs, ), dtype=float)
            myHdf5.create_dataset("/entry_1/result_1/rankID", data=np.zeros(numJobs, ), dtype=int)
            myHdf5.flush()

        ds_nPeaks = myHdf5.create_dataset("/entry_1/result_1/nPeaks",(0,),
                                          maxshape=(None,),
                                          dtype=int)
        ds_nPeaks.attrs["axes"] = "experiment_identifier"

        ds_nPeaks.attrs["minPeaks"] = args.minPeaks
        ds_nPeaks.attrs["maxPeaks"] = args.maxPeaks
        ds_nPeaks.attrs["minRes"] = args.minRes
        ds_posX = myHdf5.create_dataset("/entry_1/result_1/peakXPosRaw",(0,args.maxPeaks),
                                                 maxshape=(None,args.maxPeaks),
                                                 chunks = (1, args.maxPeaks),
                                                 compression='gzip',
                                                 compression_opts=1,
                                                 dtype=float)
        ds_posX.attrs["axes"] = "experiment_identifier:peaks"

        ds_posY = myHdf5.create_dataset("/entry_1/result_1/peakYPosRaw",(0,args.maxPeaks),
                                                 maxshape=(None,args.maxPeaks),
                                                 chunks=(1, args.maxPeaks),
                                                 compression='gzip',
                                                 compression_opts=1,
                                                 dtype=float)
        ds_posY.attrs["axes"] = "experiment_identifier:peaks"

        ds_atot = myHdf5.create_dataset("/entry_1/result_1/peakTotalIntensity",(0,args.maxPeaks),
                                                 maxshape=(None,args.maxPeaks),
                                                 chunks=(1, args.maxPeaks),
                                                 compression='gzip',
                                                 compression_opts=1,
                                                 dtype=float)
        ds_atot.attrs["axes"] = "experiment_identifier:peaks"
        ds_amax = myHdf5.create_dataset("/entry_1/result_1/peakMaxIntensity", (0,args.maxPeaks),
                                        maxshape = (None,args.maxPeaks),
                                        chunks = (1,args.maxPeaks),
                                        compression='gzip',
                                        compression_opts=1,
                                        dtype=float)
        ds_amax.attrs["axes"] = "experiment_identifier:peaks"

        ds_radius = myHdf5.create_dataset("/entry_1/result_1/peakRadius",(0,args.maxPeaks),
                                                 maxshape=(None,args.maxPeaks),
                                                 chunks=(1, args.maxPeaks),
                                                 compression='gzip',
                                                 compression_opts=1,
                                                 dtype=float)
        ds_radius.attrs["axes"] = "experiment_identifier:peaks"

        # PeakNet labels
        ds_rcentre = myHdf5.create_dataset("/entry_1/result_1/centreRow",(0,args.maxPeaks),
                                                 maxshape=(None,args.maxPeaks),
                                                 chunks=(1, args.maxPeaks),
                                                 compression='gzip',
                                                 compression_opts=1,
                                                 dtype=float)
        ds_rcentre.attrs["axes"] = "experiment_identifier:peaks"

        ds_ccentre = myHdf5.create_dataset("/entry_1/result_1/centreCol",(0,args.maxPeaks),
                                                 maxshape=(None,args.maxPeaks),
                                                 chunks=(1, args.maxPeaks),
                                                 compression='gzip',
                                                 compression_opts=1,
                                                 dtype=float)
        ds_ccentre.attrs["axes"] = "experiment_identifier:peaks"

        ds_rminPeak = myHdf5.create_dataset("/entry_1/result_1/minPeakRow",(0,args.maxPeaks),
                                                 maxshape=(None,args.maxPeaks),
                                                 chunks=(1, args.maxPeaks),
                                                 compression='gzip',
                                                 compression_opts=1,
                                                 dtype=float)
        ds_rminPeak.attrs["axes"] = "experiment_identifier:peaks"

        ds_rmaxPeak = myHdf5.create_dataset("/entry_1/result_1/maxPeakRow",(0,args.maxPeaks),
                                                 maxshape=(None,args.maxPeaks),
                                                 chunks=(1, args.maxPeaks),
                                                 compression='gzip',
                                                 compression_opts=1,
                                                 dtype=float)
        ds_rmaxPeak.attrs["axes"] = "experiment_identifier:peaks"

        ds_cminPeak = myHdf5.create_dataset("/entry_1/result_1/minPeakCol",(0,args.maxPeaks),
                                                 maxshape=(None,args.maxPeaks),
                                                 chunks=(1, args.maxPeaks),
                                                 compression='gzip',
                                                 compression_opts=1,
                                                 dtype=float)
        ds_cminPeak.attrs["axes"] = "experiment_identifier:peaks"

        ds_cmaxPeak = myHdf5.create_dataset("/entry_1/result_1/maxPeakCol",(0,args.maxPeaks),
                                                 maxshape=(None,args.maxPeaks),
                                                 chunks=(1, args.maxPeaks),
                                                 compression='gzip',
                                                 compression_opts=1,
                                                 dtype=float)
        ds_cmaxPeak.attrs["axes"] = "experiment_identifier:peaks"
        # end of PeakNet labels

        ds_maxRes = myHdf5.create_dataset("/entry_1/result_1/maxRes",(0,),
                                                 maxshape=(None,),
                                                 dtype=int)
        ds_maxRes.attrs["axes"] = "experiment_identifier:peaks"

        ds_likelihood = myHdf5.create_dataset("/entry_1/result_1/likelihood",(0,),
                                                 maxshape=(None,),
                                                 dtype=float)
        ds_likelihood.attrs["axes"] = "experiment_identifier"

        ds_timeToolDelay = myHdf5.create_dataset("/entry_1/result_1/timeToolDelay",(0,),
                                                 maxshape=(None,),
                                                 dtype=float)
        ds_timeToolDelay.attrs["axes"] = "experiment_identifier"
        ds_laserTimeZero = myHdf5.create_dataset("/entry_1/result_1/laserTimeZero",(0,),
                                                 maxshape=(None,),
                                                 dtype=float)
        ds_laserTimeZero.attrs["axes"] = "experiment_identifier"
        ds_laserTimeDelay = myHdf5.create_dataset("/entry_1/result_1/laserTimeDelay",(0,),
                                                 maxshape=(None,),
                                                 dtype=float)
        ds_laserTimeDelay.attrs["axes"] = "experiment_identifier"
        ds_laserTimePhaseLocked = myHdf5.create_dataset("/entry_1/result_1/laserTimePhaseLocked",(0,),
                                                 maxshape=(None,),
                                                 dtype=float)
        ds_laserTimePhaseLocked.attrs["axes"] = "experiment_identifier"

        myHdf5.flush()

        entry_1.create_dataset("start_time",data=ps.getStartTime())
        myHdf5.flush()

        sample_1 = entry_1.create_group("sample_1")
        sample_1.create_dataset("name",data=args.sample)
        myHdf5.flush()

        instrument_1 = entry_1.create_group("instrument_1")
        instrument_1.create_dataset("name", data=args.instrument)
        myHdf5.flush()

        source_1 = instrument_1.create_group("source_1")
        ds_photonEnergy = source_1.create_dataset("energy",(0,),
                                                 maxshape=(None,),
                                                 dtype=float) # photon energy in J
        ds_photonEnergy.attrs["axes"] = "experiment_identifier"

        ds_pulseEnergy = source_1.create_dataset("pulse_energy",(0,),
                                                 maxshape=(None,),
                                                 dtype=float) # in J
        ds_pulseEnergy.attrs["axes"] = "experiment_identifier"

        ds_pulseWidth = source_1.create_dataset("pulse_width",(0,),
                                                 maxshape=(None,),
                                                 dtype=float) # in s
        ds_pulseWidth.attrs["axes"] = "experiment_identifier"

        myHdf5.flush()

        detector_1 = instrument_1.create_group("detector_1")
        ds_data_1 = detector_1.create_dataset("data", (0, dim0, dim1),
                                    chunks=(1, dim0, dim1),
                                    maxshape=(None, dim0, dim1),
                                    compression='gzip',
                                    compression_opts=1,
                                    dtype=np.float32)              #### Change this to float32
        ds_data_1.attrs["axes"] = "experiment_identifier"

        data_1 = entry_1.create_group("data_1")
        data_1["data"] = h5py.SoftLink('/entry_1/instrument_1/detector_1/data')

        # Add x,y,z coordinates
        cx, cy, cz = ps.det.coords_xyz(ps.evt)
        ds_x = data_1.create_dataset("x", (dim0, dim1),
                                    chunks=(dim0, dim1),
                                    maxshape=(dim0, dim1),
                                    compression='gzip',
                                    compression_opts=1,
                                    dtype=float)
        ds_y = data_1.create_dataset("y", (dim0, dim1),
                                    chunks=(dim0, dim1),
                                    maxshape=(dim0, dim1),
                                    compression='gzip',
                                    compression_opts=1,
                                    dtype=float)
        ds_z = data_1.create_dataset("z", (dim0, dim1),
                                    chunks=(dim0, dim1),
                                    maxshape=(dim0, dim1),
                                    compression='gzip',
                                    compression_opts=1,
                                    dtype=float)
        ds_x = ps.getCheetahImg(calib=cx)
        ds_y = ps.getCheetahImg(calib=cy)
        ds_z = ps.getCheetahImg(calib=cz)

        # Add mask in cheetah format
        if args.mask is not None:
            ds_mask_1 = data_1.create_dataset("mask", (0, dim0, dim1),
                                        chunks=(1, dim0, dim1),
                                        maxshape=(None, dim0, dim1),
                                        compression='gzip',
                                        compression_opts=1,
                                        dtype=int)

        ds_dist_1 = detector_1.create_dataset("distance",(0,),
                                                 maxshape=(None,),
                                                 dtype=float) # in meters
        ds_dist_1.attrs["axes"] = "experiment_identifier"

        ds_x_pixel_size_1 = detector_1.create_dataset("x_pixel_size",(0,),
                                                 maxshape=(None,),
                                                 dtype=float)
        ds_x_pixel_size_1.attrs["axes"] = "experiment_identifier"

        ds_y_pixel_size_1 = detector_1.create_dataset("y_pixel_size",(0,),
                                                 maxshape=(None,),
                                                 dtype=float)
        ds_y_pixel_size_1.attrs["axes"] = "experiment_identifier"

        detector_1.create_dataset("description",data=args.det)
        myHdf5.flush()

    elif facility == 'PAL':
        entry_1 = myHdf5.create_group("entry_1")
        ds_expId = entry_1.create_dataset("experimental_identifier", (0,),
                                          maxshape=(None,),
                                          dtype=int)
        ds_expId.attrs["axes"] = "experiment_identifier"

        myHdf5.create_dataset("/entry_1/result_1/nPeaksAll", data=np.ones(numJobs, ) * -1, dtype=int)
        myHdf5.create_dataset("/entry_1/result_1/peakXPosRawAll", (numJobs, args.maxPeaks), dtype=float, chunks=(1, args.maxPeaks))
        myHdf5.create_dataset("/entry_1/result_1/peakYPosRawAll", (numJobs, args.maxPeaks), dtype=float, chunks=(1, args.maxPeaks))
        myHdf5.create_dataset("/entry_1/result_1/peakTotalIntensityAll", (numJobs, args.maxPeaks), dtype=float, chunks=(1,args.maxPeaks))
        myHdf5.create_dataset("/entry_1/result_1/peakMaxIntensityAll", (numJobs, args.maxPeaks), dtype=float, chunks=(1, args.maxPeaks))
        myHdf5.create_dataset("/entry_1/result_1/peakRadiusAll", (numJobs, args.maxPeaks), dtype=float, chunks=(1, args.maxPeaks))
        myHdf5.create_dataset("/entry_1/result_1/maxResAll", data=np.ones(numJobs, ) * -1, dtype=int)
        myHdf5.flush()

        if args.profile:
            myHdf5.create_dataset("/entry_1/result_1/calibTime", data=np.zeros(numJobs, ), dtype=float)
            myHdf5.create_dataset("/entry_1/result_1/peakTime", data=np.zeros(numJobs, ), dtype=float)
            myHdf5.create_dataset("/entry_1/result_1/saveTime", data=np.zeros(numJobs, ), dtype=float)
            myHdf5.create_dataset("/entry_1/result_1/reshapeTime", (0,), maxshape=(None,), dtype=float)
            myHdf5.create_dataset("/entry_1/result_1/totalTime", data=np.zeros(numJobs, ), dtype=float)
            myHdf5.create_dataset("/entry_1/result_1/rankID", data=np.zeros(numJobs, ), dtype=int)
            myHdf5.flush()

        ds_nPeaks = myHdf5.create_dataset("/entry_1/result_1/nPeaks", (0,),
                                          maxshape=(None,),
                                          dtype=int)
        ds_nPeaks.attrs["axes"] = "experiment_identifier"

        ds_nPeaks.attrs["minPeaks"] = args.minPeaks
        ds_nPeaks.attrs["maxPeaks"] = args.maxPeaks
        ds_nPeaks.attrs["minRes"] = args.minRes
        ds_posX = myHdf5.create_dataset("/entry_1/result_1/peakXPosRaw", (0, args.maxPeaks),
                                        maxshape=(None, args.maxPeaks),
                                        chunks=(1, args.maxPeaks),
                                        compression='gzip',
                                        compression_opts=1,
                                        dtype=float)
        ds_posX.attrs["axes"] = "experiment_identifier:peaks"

        ds_posY = myHdf5.create_dataset("/entry_1/result_1/peakYPosRaw", (0, args.maxPeaks),
                                        maxshape=(None, args.maxPeaks),
                                        chunks=(1, args.maxPeaks),
                                        compression='gzip',
                                        compression_opts=1,
                                        dtype=float)
        ds_posY.attrs["axes"] = "experiment_identifier:peaks"

        ds_atot = myHdf5.create_dataset("/entry_1/result_1/peakTotalIntensity", (0, args.maxPeaks),
                                        maxshape = (None, args.maxPeaks),
                                        chunks=(1,args.maxPeaks),
                                        compression = 'gzip',
                                        compression_opts=1,
                                        dtype=float)
        ds_atot.attrs["axes"] = "experiment_identifier:peaks"

        ds_amax = myHdf5.create_dataset("/entry_1/result_1/peakMaxIntensity", (0, args.maxPeaks),
                                        maxshape=(None, args.maxPeaks),
                                        chunks=(1, args.maxPeaks),
                                        compression='gzip',
                                        compression_opts=1,
                                        dtype=float)
        ds_amax.attrs["axes"] = "experiment_identifier:peaks"

        ds_radius = myHdf5.create_dataset("/entry_1/result_1/peakRadius", (0, args.maxPeaks),
                                        maxshape=(None, args.maxPeaks),
                                        chunks=(1, args.maxPeaks),
                                        compression='gzip',
                                        compression_opts=1,
                                        dtype=float)
        ds_radius.attrs["axes"] = "experiment_identifier:peaks"

        ds_maxRes = myHdf5.create_dataset("/entry_1/result_1/maxRes", (0,),
                                          maxshape=(None,),
                                          dtype=int)
        ds_maxRes.attrs["axes"] = "experiment_identifier:peaks"
        myHdf5.flush()

        sample_1 = entry_1.create_group("sample_1")
        sample_1.create_dataset("name", data=args.sample)
        myHdf5.flush()

        instrument_1 = entry_1.create_group("instrument_1")
        instrument_1.create_dataset("name", data='PAL') #FIXME: change to beamline name
        myHdf5.flush()

        detector_1 = instrument_1.create_group("detector_1")
        ds_data_1 = detector_1.create_dataset("data", (0, dim0, dim1),
                                              chunks=(1, dim0, dim1),
                                              maxshape=(None, dim0, dim1),
                                              compression='gzip',
                                              compression_opts=1,
                                              dtype=float)
        ds_data_1.attrs["axes"] = "experiment_identifier"

        data_1 = entry_1.create_group("data_1")
        data_1["data"] = h5py.SoftLink('/entry_1/instrument_1/detector_1/data')

        # Add mask in cheetah format
        if args.mask is not None:
            ds_mask_1 = data_1.create_dataset("mask", (0, dim0, dim1),
                                        chunks=(1, dim0, dim1),
                                        maxshape=(None, dim0, dim1),
                                        compression='gzip',
                                        compression_opts=1,
                                        dtype=int)

        ds_dist_1 = detector_1.create_dataset("distance", (0,),
                                              maxshape=(None,),
                                              dtype=float)  # in meters
        ds_dist_1.attrs["axes"] = "experiment_identifier"

    # Close hdf5 file
    myHdf5.close()

comm.Barrier()

if rank==0:
    runmaster(args, numClients)
else:
    print("Using auto peak finder: ", str2bool(args.auto))
    runclientAuto(args)

MPI.Finalize()
