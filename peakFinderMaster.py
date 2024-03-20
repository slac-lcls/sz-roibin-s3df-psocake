import h5py
from mpidata import mpidata
import time
import numpy as np
import os
from utils import *

from mpi4py import MPI
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

if 'PSOCAKE_FACILITY' not in os.environ: os.environ['PSOCAKE_FACILITY'] = 'LCLS' # Default facility
if 'LCLS' in os.environ['PSOCAKE_FACILITY'].upper():
    facility = 'LCLS'
    from PSCalib.GeometryObject import two2x1ToData2x2
    import psana

def runmaster(args, nClients):

    runStr = "%04d" % args.run
    fname = args.outDir +"/"+ args.exp +"_"+ runStr
    if args.tag: fname += '_' + args.tag
    fname += ".cxi"

    grpName = "/entry_1/result_1"
    dset_nPeaks = "/nPeaksAll"
    dset_posX = "/peakXPosRawAll"
    dset_posY = "/peakYPosRawAll"
    dset_atot = "/peakTotalIntensityAll"
    dset_amax = "/peakMaxIntensityAll"
    dset_radius = "/peakRadiusAll"
    dset_rcentre = "/centreRowAll"
    dset_ccentre = "/centreColAll"
    dset_rminPeak = "/minPeakRowAll"
    dset_rmaxPeak = "/maxPeakRowAll"
    dset_cminPeak = "/minPeakColAll"
    dset_cmaxPeak = "/maxPeakColAll"
    dset_maxRes = "/maxResAll"
    dset_likelihood = "/likelihoodAll"
    dset_timeToolDelay = "/timeToolDelayAll"
    dset_laserTimeZero = "/laserTimeZeroAll"
    dset_laserTimeDelay = "/laserTimeDelayAll"
    dset_laserTimePhaseLocked = "/laserTimePhaseLockedAll"
    dset_saveTime = "/saveTime"
    dset_calibTime = "/calibTime"
    dset_peakTime = "/peakTime"
    dset_totalTime = "/totalTime"
    dset_rankID = "/rankID"
    statusFname = args.outDir + "/status_peaks"
    if args.tag: statusFname += '_'+args.tag
    statusFname += ".txt"

    powderHits = None
    powderMisses = None

    maxSize = 0
    numInc = 0
    inc = 1000
    dataShape = (0,0)
    numProcessed = 0
    numHits = 0
    hitRate = 0.0
    fracDone = 0.0
    projected = 0.0
    likelihood = 0.0
    timeToolDelay = 0.0
    laserTimeZero = 0.0
    laserTimeDelay = 0.0
    laserTimePhaseLocked = 0.0
    numEvents = getNoe(args, facility)
    d = {"numHits": numHits, "hitRate(%)": hitRate, "fracDone(%)": fracDone, "projected": projected}
    try:  
        writeStatus(statusFname, d)
    except:
        pass

    # Init mask
    mask = None
    if args.mask is not None:
        f = h5py.File(args.mask, 'r')
        mask = f['/entry_1/data_1/mask'][()]
        f.close()
        mask = -1*(mask-1)

    myHdf5 = h5py.File(fname, 'r+')
    numLeft = 1
    while nClients > 0 and numLeft > 0:
        # Remove client if the run ended
        md = mpidata()
        md.recv()
        if md.small.endrun:
            nClients -= 1
        elif hasattr(md.small, 'powder') and md.small.powder == 1 and hasattr(md, "powderHits"):
            if powderHits is None:
                powderHits = md.powderHits
                powderMisses = md.powderMisses
            else:
                powderHits = np.maximum(powderHits, md.powderHits)
                powderMisses = np.maximum(powderMisses, md.powderMisses)
        else:
            try:
                nPeaks = md.peaks.shape[0]
                maxRes = md.small.maxRes

                # FIXME
                if facility == 'LCLS' and numHits > 0:
                    alreadyDone = len(np.where(myHdf5["/LCLS/eventNumber"][:numHits] == md.small.eventNum)[0])
                    if alreadyDone >= 1: continue

                if args.profile:
                    calibTime = md.small.calibTime
                    peakTime = md.small.peakTime
                    totalTime = md.small.totalTime
                    rankID = md.small.rankID
            except:
                if hasattr(md.small, "eventNum"):
                    myHdf5[grpName + dset_nPeaks][md.small.eventNum] = -2
                numLeft = len(np.where(myHdf5[grpName + dset_nPeaks][()] == -1)[0])
                continue

            if nPeaks > args.maxPeaks: # only save upto maxNumPeaks
                md.peaks = md.peaks[:args.maxPeaks]
                nPeaks = md.peaks.shape[0]
                md.radius = md.radius[:args.maxPeaks]

            ran = True
            if ran:
                rad = np.zeros(args.maxPeaks)
                rad[:nPeaks] = md.radius
                ran = False

            if args.profile: tic = time.time()

            if facility == 'LCLS':
                for i,peak in enumerate(md.peaks):
                    #seg, row, col, npix, atot, son = peak
                    seg,row,col,npix,amax,atot,rcent,ccent,rsigma,csigma,rmin,rmax,cmin,cmax,bkgd,rms,son = peak[0:17]
                    cheetahRow,cheetahCol = convert_peaks_to_cheetah(seg,row,col)
                    myHdf5[grpName+dset_posX][md.small.eventNum,i] = cheetahCol
                    myHdf5[grpName+dset_posY][md.small.eventNum,i] = cheetahRow
                    myHdf5[grpName+dset_atot][md.small.eventNum,i] = atot
                    myHdf5[grpName+dset_amax][md.small.eventNum,i] = amax
                    myHdf5[grpName+dset_radius][md.small.eventNum,i] = rad[i]
                    # PeakNet training labels
                    myHdf5[grpName+dset_rcentre][md.small.eventNum,i] = rcent
                    myHdf5[grpName+dset_ccentre][md.small.eventNum,i] = ccent
                    myHdf5[grpName+dset_rminPeak][md.small.eventNum,i] = rmin
                    myHdf5[grpName+dset_rmaxPeak][md.small.eventNum,i] = rmax
                    myHdf5[grpName+dset_cminPeak][md.small.eventNum,i] = cmin
                    myHdf5[grpName+dset_cmaxPeak][md.small.eventNum,i] = cmax
                    myHdf5.flush()

            myHdf5[grpName+dset_nPeaks][md.small.eventNum] = nPeaks
            myHdf5[grpName+dset_maxRes][md.small.eventNum] = maxRes

            numLeft = len(np.where(myHdf5[grpName + dset_nPeaks][()] == -1)[0])

            likelihood = md.small.likelihood
            myHdf5[grpName + dset_likelihood][md.small.eventNum] = likelihood

            if facility == 'LCLS':
                myHdf5[grpName + dset_timeToolDelay][md.small.eventNum] = md.small.timeToolDelay
                myHdf5[grpName + dset_laserTimeZero][md.small.eventNum] = md.small.laserTimeZero
                myHdf5[grpName + dset_laserTimeDelay][md.small.eventNum] = md.small.laserTimeDelay
                myHdf5[grpName + dset_laserTimePhaseLocked][md.small.eventNum] = md.small.laserTimePhaseLocked
            myHdf5.flush()

            if args.profile:
                saveTime = time.time() - tic # Time to save the peaks found per event
                myHdf5[grpName + dset_calibTime][md.small.eventNum] = calibTime
                myHdf5[grpName + dset_peakTime][md.small.eventNum] = peakTime
                myHdf5[grpName + dset_saveTime][md.small.eventNum] = saveTime
                myHdf5[grpName + dset_totalTime][md.small.eventNum] = totalTime
                myHdf5[grpName + dset_rankID][md.small.eventNum] = rankID
                myHdf5.flush()

            # If the event is a hit
            if nPeaks >= args.minPeaks and \
               nPeaks <= args.maxPeaks and \
               maxRes >= args.minRes and \
               hasattr(md, 'data'):
                if facility == 'LCLS':
                    # Assign a bigger array
                    if maxSize == numHits:
                        if args.profile: tic = time.time()
                        reshapeHdf5(myHdf5, '/entry_1/result_1/nPeaks', numHits, inc)
                        myHdf5["/entry_1/result_1/peakXPosRaw"].resize((numHits + inc, args.maxPeaks))
                        myHdf5["/entry_1/result_1/peakYPosRaw"].resize((numHits + inc, args.maxPeaks))
                        myHdf5["/entry_1/result_1/peakTotalIntensity"].resize((numHits + inc, args.maxPeaks))
                        myHdf5["/entry_1/result_1/peakMaxIntensity"].resize((numHits + inc, args.maxPeaks))
                        myHdf5["/entry_1/result_1/peakRadius"].resize((numHits + inc, args.maxPeaks))
                        myHdf5["/entry_1/result_1/centreRow"].resize((numHits + inc, args.maxPeaks))
                        myHdf5["/entry_1/result_1/centreCol"].resize((numHits + inc, args.maxPeaks))
                        myHdf5["/entry_1/result_1/minPeakRow"].resize((numHits + inc, args.maxPeaks))
                        myHdf5["/entry_1/result_1/maxPeakRow"].resize((numHits + inc, args.maxPeaks))
                        myHdf5["/entry_1/result_1/minPeakCol"].resize((numHits + inc, args.maxPeaks))
                        myHdf5["/entry_1/result_1/maxPeakCol"].resize((numHits + inc, args.maxPeaks))
                        reshapeHdf5(myHdf5, '/entry_1/result_1/maxRes', numHits, inc)
                        reshapeHdf5(myHdf5, '/entry_1/result_1/likelihood', numHits, inc)
                        reshapeHdf5(myHdf5, '/entry_1/result_1/timeToolDelay', numHits, inc)
                        reshapeHdf5(myHdf5, '/entry_1/result_1/laserTimeZero', numHits, inc)
                        reshapeHdf5(myHdf5, '/entry_1/result_1/laserTimeDelay', numHits, inc)
                        reshapeHdf5(myHdf5, '/entry_1/result_1/laserTimePhaseLocked', numHits, inc)
                        reshapeHdf5(myHdf5, '/entry_1/instrument_1/source_1/pulse_width', numHits, inc)
                        reshapeHdf5(myHdf5, '/LCLS/photon_energy_eV', numHits, inc)
                        reshapeHdf5(myHdf5, '/entry_1/instrument_1/source_1/energy', numHits, inc) # J
                        reshapeHdf5(myHdf5, '/entry_1/instrument_1/source_1/pulse_energy', numHits, inc)
                        reshapeHdf5(myHdf5, '/entry_1/instrument_1/detector_1/distance', numHits, inc)
                        reshapeHdf5(myHdf5, '/entry_1/instrument_1/detector_1/x_pixel_size', numHits, inc)
                        reshapeHdf5(myHdf5, '/entry_1/instrument_1/detector_1/y_pixel_size', numHits, inc)
                        reshapeHdf5(myHdf5, '/LCLS/detector_1/EncoderValue', numHits, inc)
                        reshapeHdf5(myHdf5, '/LCLS/detector_1/electronBeamEnergy', numHits, inc)
                        reshapeHdf5(myHdf5, '/LCLS/detector_1/beamRepRate', numHits, inc)
                        reshapeHdf5(myHdf5, '/LCLS/detector_1/particleN_electrons', numHits, inc)
                        reshapeHdf5(myHdf5, '/LCLS/injector_1/pressureSDS', numHits, inc)
                        reshapeHdf5(myHdf5, '/LCLS/injector_1/pressureSDSB', numHits, inc)
                        if hasattr(md, 'evr0'):
                            reshapeHdf5(myHdf5, '/LCLS/detector_1/evr0', numHits, inc)
                        if hasattr(md, 'evr1'):
                            reshapeHdf5(myHdf5, '/LCLS/detector_1/evr1', numHits, inc)
                        reshapeHdf5(myHdf5, '/LCLS/eVernier', numHits, inc)
                        reshapeHdf5(myHdf5, '/LCLS/charge', numHits, inc)
                        reshapeHdf5(myHdf5, '/LCLS/peakCurrentAfterSecondBunchCompressor', numHits, inc)
                        reshapeHdf5(myHdf5, '/LCLS/pulseLength', numHits, inc)
                        reshapeHdf5(myHdf5, '/LCLS/ebeamEnergyLossConvertedToPhoton_mJ', numHits, inc)
                        reshapeHdf5(myHdf5, '/LCLS/calculatedNumberOfPhotons', numHits, inc)
                        reshapeHdf5(myHdf5, '/LCLS/photonBeamEnergy', numHits, inc)
                        reshapeHdf5(myHdf5, '/LCLS/wavelength', numHits, inc)
                        reshapeHdf5(myHdf5, '/LCLS/photon_wavelength_A', numHits, inc)
                        reshapeHdf5(myHdf5, '/LCLS/machineTime', numHits, inc)
                        reshapeHdf5(myHdf5, '/LCLS/machineTimeNanoSeconds', numHits, inc)
                        reshapeHdf5(myHdf5, '/LCLS/fiducial', numHits, inc)
                        reshapeHdf5(myHdf5, '/LCLS/eventNumber', numHits, inc)
                        reshapeHdf5(myHdf5, '/LCLS/ttspecAmpl', numHits, inc)
                        reshapeHdf5(myHdf5, '/LCLS/ttspecAmplNxt', numHits, inc)
                        reshapeHdf5(myHdf5, '/LCLS/ttspecFltPos', numHits, inc)
                        reshapeHdf5(myHdf5, '/LCLS/ttspecFltPosFwhm', numHits, inc)
                        reshapeHdf5(myHdf5, '/LCLS/ttspecFltPosPs', numHits, inc)
                        reshapeHdf5(myHdf5, '/LCLS/ttspecRefAmpl', numHits, inc)
                        reshapeHdf5(myHdf5, '/entry_1/experimental_identifier', numHits, inc) # same as /LCLS/eventNumber
                        dataShape = md.data.shape
                        myHdf5["/entry_1/data_1/data"].resize((numHits + inc, md.data.shape[0], md.data.shape[1]))
                        if args.mask is not None:
                            myHdf5["/entry_1/data_1/mask"].resize((numHits + inc, md.data.shape[0], md.data.shape[1]))
                        if args.profile:
                            reshapeHdf5(myHdf5, '/entry_1/result_1/reshapeTime', numInc, 1)
                            reshapeTime = time.time() - tic
                            updateHdf5(myHdf5, '/entry_1/result_1/reshapeTime', numInc, reshapeTime)
                        maxSize += inc
                        numInc += 1

                    # Save peak information
                    updateHdf5(myHdf5, '/entry_1/result_1/nPeaks', numHits, nPeaks)
                    myHdf5["/entry_1/result_1/peakXPosRaw"][numHits,:] = myHdf5[grpName+dset_posX][md.small.eventNum,:]
                    myHdf5["/entry_1/result_1/peakYPosRaw"][numHits,:] = myHdf5[grpName+dset_posY][md.small.eventNum,:]
                    myHdf5["/entry_1/result_1/peakTotalIntensity"][numHits,:] = myHdf5[grpName+dset_atot][md.small.eventNum,:]
                    myHdf5["/entry_1/result_1/peakMaxIntensity"][numHits,:] = myHdf5[grpName+dset_amax][md.small.eventNum,:]
                    myHdf5["/entry_1/result_1/peakRadius"][numHits,:] = myHdf5[grpName+dset_radius][md.small.eventNum,:]
                    # Save PeakNet labels
                    myHdf5["/entry_1/result_1/centreRow"][numHits,:] = myHdf5[grpName+dset_rcentre][md.small.eventNum,:]
                    myHdf5["/entry_1/result_1/centreCol"][numHits,:] = myHdf5[grpName+dset_ccentre][md.small.eventNum,:]
                    myHdf5["/entry_1/result_1/minPeakRow"][numHits,:] = myHdf5[grpName+dset_rminPeak][md.small.eventNum,:]
                    myHdf5["/entry_1/result_1/maxPeakRow"][numHits,:] = myHdf5[grpName+dset_rmaxPeak][md.small.eventNum,:]
                    myHdf5["/entry_1/result_1/minPeakCol"][numHits,:] = myHdf5[grpName+dset_cminPeak][md.small.eventNum,:]
                    myHdf5["/entry_1/result_1/maxPeakCol"][numHits,:] = myHdf5[grpName+dset_cmaxPeak][md.small.eventNum,:]

                    updateHdf5(myHdf5, '/entry_1/result_1/maxRes', numHits, maxRes)
                    updateHdf5(myHdf5, '/entry_1/result_1/likelihood', numHits, likelihood)
                    # Save epics
                    updateHdf5(myHdf5, '/entry_1/result_1/timeToolDelay', numHits, md.small.timeToolDelay)
                    updateHdf5(myHdf5, '/entry_1/result_1/laserTimeZero', numHits, md.small.laserTimeZero)
                    updateHdf5(myHdf5, '/entry_1/result_1/laserTimeDelay', numHits, md.small.laserTimeDelay)
                    updateHdf5(myHdf5, '/entry_1/result_1/laserTimePhaseLocked', numHits, md.small.laserTimePhaseLocked)
                    updateHdf5(myHdf5, '/entry_1/instrument_1/source_1/pulse_width', numHits, md.small.pulseLength)
                    updateHdf5(myHdf5, '/LCLS/photon_energy_eV', numHits, md.small.photonEnergy)
                    if md.small.photonEnergy is not None:
                        updateHdf5(myHdf5, '/entry_1/instrument_1/source_1/energy', numHits, md.small.photonEnergy * 1.60218e-19) # J
                    else:
                        updateHdf5(myHdf5, '/entry_1/instrument_1/source_1/energy', numHits, 0.)
                    updateHdf5(myHdf5, '/entry_1/instrument_1/source_1/pulse_energy', numHits, md.small.pulseEnergy)
                    updateHdf5(myHdf5, '/entry_1/instrument_1/detector_1/distance', numHits, md.small.detectorDistance)
                    updateHdf5(myHdf5, '/entry_1/instrument_1/detector_1/x_pixel_size', numHits, args.pixelSize)
                    updateHdf5(myHdf5, '/entry_1/instrument_1/detector_1/y_pixel_size', numHits, args.pixelSize)
                    updateHdf5(myHdf5, '/LCLS/detector_1/EncoderValue', numHits, md.small.lclsDet)
                    updateHdf5(myHdf5, '/LCLS/detector_1/electronBeamEnergy', numHits, md.small.ebeamCharge)
                    updateHdf5(myHdf5, '/LCLS/detector_1/beamRepRate', numHits, md.small.beamRepRate)
                    updateHdf5(myHdf5, '/LCLS/detector_1/particleN_electrons', numHits, md.small.particleN_electrons)
                    updateHdf5(myHdf5, '/LCLS/injector_1/pressureSDS', numHits, md.small.injectorPressureSDS)
                    updateHdf5(myHdf5, '/LCLS/injector_1/pressureSDSB', numHits, md.small.injectorPressureSDSB)
                    if hasattr(md, 'evr0'):
                        updateHdf5(myHdf5, '/LCLS/detector_1/evr0', numHits, md.evr0)
                    if hasattr(md, 'evr1'):
                        updateHdf5(myHdf5, '/LCLS/detector_1/evr1', numHits, md.evr1)
                    updateHdf5(myHdf5, '/LCLS/eVernier', numHits, md.small.eVernier)
                    updateHdf5(myHdf5, '/LCLS/charge', numHits, md.small.charge)
                    updateHdf5(myHdf5, '/LCLS/peakCurrentAfterSecondBunchCompressor', numHits, md.small.peakCurrentAfterSecondBunchCompressor)
                    updateHdf5(myHdf5, '/LCLS/pulseLength', numHits, md.small.pulseLength)
                    updateHdf5(myHdf5, '/LCLS/ebeamEnergyLossConvertedToPhoton_mJ', numHits, md.small.ebeamEnergyLossConvertedToPhoton_mJ)
                    updateHdf5(myHdf5, '/LCLS/calculatedNumberOfPhotons', numHits, md.small.calculatedNumberOfPhotons)
                    updateHdf5(myHdf5, '/LCLS/photonBeamEnergy', numHits, md.small.photonBeamEnergy)
                    updateHdf5(myHdf5, '/LCLS/wavelength', numHits, md.small.wavelength)
                    if md.small.wavelength is not None:
                        updateHdf5(myHdf5, '/LCLS/photon_wavelength_A', numHits, md.small.wavelength * 10.)
                    else:
                        updateHdf5(myHdf5, '/LCLS/photon_wavelength_A', numHits, 0.)
                    updateHdf5(myHdf5, '/LCLS/machineTime', numHits, md.small.sec)
                    updateHdf5(myHdf5, '/LCLS/machineTimeNanoSeconds', numHits, md.small.nsec)
                    updateHdf5(myHdf5, '/LCLS/fiducial', numHits, md.small.fid)
                    updateHdf5(myHdf5, '/LCLS/eventNumber', numHits, md.small.eventNum)
                    updateHdf5(myHdf5, '/LCLS/ttspecAmpl', numHits, md.small.ttspecAmpl)
                    updateHdf5(myHdf5, '/LCLS/ttspecAmplNxt', numHits, md.small.ttspecAmplNxt)
                    updateHdf5(myHdf5, '/LCLS/ttspecFltPos', numHits, md.small.ttspecFltPos)
                    updateHdf5(myHdf5, '/LCLS/ttspecFltPosFwhm', numHits, md.small.ttspecFltPosFwhm)
                    updateHdf5(myHdf5, '/LCLS/ttspecFltPosPs', numHits, md.small.ttspecFltPosPs)
                    updateHdf5(myHdf5, '/LCLS/ttspecRefAmpl', numHits, md.small.ttspecRefAmpl)
                    updateHdf5(myHdf5, '/entry_1/experimental_identifier', numHits, md.small.eventNum) # same as /LCLS/eventNumber
                    # Save images
                    myHdf5["/entry_1/data_1/data"][numHits, :, :] = md.data
                    if mask is not None:
                        myHdf5["/entry_1/data_1/mask"][numHits, :, :] = mask
                    numHits += 1
                    myHdf5.flush()

            numProcessed += 1
            # Update status
            if numProcessed % 60:
                try:
                    hitRate = numHits * 100. / numProcessed
                    fracDone = numProcessed * 100. / numEvents
                    projected = hitRate / 100. * numEvents
                    d = {"numHits": numHits, "hitRate(%)": round(hitRate,3), "fracDone(%)": round(fracDone,3), "projected": round(projected)}
                    writeStatus(statusFname, d)
                except:
                    pass

    # Crop back to the correct size
    if facility == 'LCLS':
        cropHdf5(myHdf5, '/entry_1/result_1/nPeaks', numHits)
        myHdf5["/entry_1/result_1/peakXPosRaw"].resize((numHits, args.maxPeaks))
        myHdf5["/entry_1/result_1/peakYPosRaw"].resize((numHits, args.maxPeaks))
        myHdf5["/entry_1/result_1/peakTotalIntensity"].resize((numHits, args.maxPeaks))
        myHdf5["/entry_1/result_1/peakMaxIntensity"].resize((numHits, args.maxPeaks))
        myHdf5["/entry_1/result_1/peakRadius"].resize((numHits, args.maxPeaks))
        myHdf5["/entry_1/result_1/centreRow"].resize((numHits, args.maxPeaks))
        myHdf5["/entry_1/result_1/centreCol"].resize((numHits, args.maxPeaks))
        myHdf5["/entry_1/result_1/minPeakRow"].resize((numHits, args.maxPeaks))
        myHdf5["/entry_1/result_1/maxPeakRow"].resize((numHits, args.maxPeaks))
        myHdf5["/entry_1/result_1/minPeakCol"].resize((numHits, args.maxPeaks))
        myHdf5["/entry_1/result_1/maxPeakCol"].resize((numHits, args.maxPeaks))
        cropHdf5(myHdf5, '/entry_1/result_1/maxRes', numHits)
        cropHdf5(myHdf5, '/entry_1/result_1/likelihood', numHits)
        cropHdf5(myHdf5, '/entry_1/result_1/timeToolDelay', numHits)
        cropHdf5(myHdf5, '/entry_1/result_1/laserTimeZero', numHits)
        cropHdf5(myHdf5, '/entry_1/result_1/laserTimeDelay', numHits)
        cropHdf5(myHdf5, '/entry_1/result_1/laserTimePhaseLocked', numHits)
        cropHdf5(myHdf5, '/entry_1/instrument_1/source_1/pulse_width', numHits)
        cropHdf5(myHdf5, '/LCLS/photon_energy_eV', numHits)
        cropHdf5(myHdf5, '/entry_1/instrument_1/source_1/energy', numHits)
        cropHdf5(myHdf5, '/entry_1/instrument_1/source_1/pulse_energy', numHits)
        cropHdf5(myHdf5, '/entry_1/instrument_1/detector_1/distance', numHits)
        cropHdf5(myHdf5, '/entry_1/instrument_1/detector_1/x_pixel_size', numHits)
        cropHdf5(myHdf5, '/entry_1/instrument_1/detector_1/y_pixel_size', numHits)
        cropHdf5(myHdf5, '/LCLS/detector_1/EncoderValue', numHits)
        cropHdf5(myHdf5, '/LCLS/detector_1/electronBeamEnergy', numHits)
        cropHdf5(myHdf5, '/LCLS/detector_1/beamRepRate', numHits)
        cropHdf5(myHdf5, '/LCLS/detector_1/particleN_electrons', numHits)
        if hasattr(md, 'evr0'):
            cropHdf5(myHdf5, '/LCLS/detector_1/evr0', numHits)
        if hasattr(md, 'evr1'):
            cropHdf5(myHdf5, '/LCLS/detector_1/evr1', numHits)
        cropHdf5(myHdf5, '/LCLS/eVernier', numHits)
        cropHdf5(myHdf5, '/LCLS/charge', numHits)
        cropHdf5(myHdf5, '/LCLS/peakCurrentAfterSecondBunchCompressor', numHits)
        cropHdf5(myHdf5, '/LCLS/pulseLength', numHits)
        cropHdf5(myHdf5, '/LCLS/ebeamEnergyLossConvertedToPhoton_mJ', numHits)
        cropHdf5(myHdf5, '/LCLS/calculatedNumberOfPhotons', numHits)
        cropHdf5(myHdf5, '/LCLS/photonBeamEnergy', numHits)
        cropHdf5(myHdf5, '/LCLS/wavelength', numHits)
        cropHdf5(myHdf5, '/LCLS/photon_wavelength_A', numHits)
        cropHdf5(myHdf5, '/LCLS/machineTime', numHits)
        cropHdf5(myHdf5, '/LCLS/machineTimeNanoSeconds', numHits)
        cropHdf5(myHdf5, '/LCLS/fiducial', numHits)
        cropHdf5(myHdf5, '/LCLS/eventNumber', numHits)
        cropHdf5(myHdf5, '/entry_1/experimental_identifier', numHits)  # same as /LCLS/eventNumber
        myHdf5["/entry_1/data_1/data"].resize((numHits, dataShape[0], dataShape[1]))
        if args.mask is not None:
            myHdf5["/entry_1/data_1/mask"].resize((numHits, dataShape[0], dataShape[1]))
        if args.profile:
            cropHdf5(myHdf5, '/entry_1/result_1/reshapeTime', numInc)
        cropHdf5(myHdf5, '/LCLS/ttspecAmpl', numHits)
        cropHdf5(myHdf5, '/LCLS/ttspecAmplNxt', numHits)
        cropHdf5(myHdf5, '/LCLS/ttspecFltPos', numHits)
        cropHdf5(myHdf5, '/LCLS/ttspecFltPosFwhm', numHits)
        cropHdf5(myHdf5, '/LCLS/ttspecFltPosPs', numHits)
        cropHdf5(myHdf5, '/LCLS/ttspecRefAmpl', numHits)

        # Save attributes
        myHdf5["LCLS/detector_1/EncoderValue"].attrs["numEvents"] = numHits
        myHdf5["LCLS/detector_1/electronBeamEnergy"].attrs["numEvents"] = numHits
        myHdf5["LCLS/detector_1/beamRepRate"].attrs["numEvents"] = numHits
        myHdf5["LCLS/detector_1/particleN_electrons"].attrs["numEvents"] = numHits
        myHdf5["LCLS/eVernier"].attrs["numEvents"] = numHits
        myHdf5["LCLS/charge"].attrs["numEvents"] = numHits
        myHdf5["LCLS/peakCurrentAfterSecondBunchCompressor"].attrs["numEvents"] = numHits
        myHdf5["LCLS/pulseLength"].attrs["numEvents"] = numHits
        myHdf5["LCLS/ebeamEnergyLossConvertedToPhoton_mJ"].attrs["numEvents"] = numHits
        myHdf5["LCLS/calculatedNumberOfPhotons"].attrs["numEvents"] = numHits
        myHdf5["LCLS/photonBeamEnergy"].attrs["numEvents"] = numHits
        myHdf5["LCLS/wavelength"].attrs["numEvents"] = numHits
        myHdf5["LCLS/machineTime"].attrs["numEvents"] = numHits
        myHdf5["LCLS/machineTimeNanoSeconds"].attrs["numEvents"] = numHits
        myHdf5["LCLS/fiducial"].attrs["numEvents"] = numHits
        myHdf5["LCLS/photon_energy_eV"].attrs["numEvents"] = numHits
        myHdf5["LCLS/photon_wavelength_A"].attrs["numEvents"] = numHits
        myHdf5["LCLS/eventNumber"].attrs["numEvents"] = numHits
        myHdf5["entry_1/experimental_identifier"].attrs["numEvents"] = numHits
        myHdf5["/entry_1/result_1/nPeaks"].attrs["numEvents"] = numHits
        myHdf5["/entry_1/result_1/peakXPosRaw"].attrs["numEvents"] = numHits
        myHdf5["/entry_1/result_1/peakYPosRaw"].attrs["numEvents"] = numHits
        myHdf5["/entry_1/result_1/peakTotalIntensity"].attrs["numEvents"] = numHits
        myHdf5["/entry_1/result_1/peakMaxIntensity"].attrs["numEvents"] = numHits
        myHdf5["/entry_1/result_1/peakRadius"].attrs["numEvents"] = numHits
        myHdf5["/entry_1/result_1/centreRow"].attrs["numEvents"] = numHits
        myHdf5["/entry_1/result_1/centreCol"].attrs["numEvents"] = numHits
        myHdf5["/entry_1/result_1/minPeakRow"].attrs["numEvents"] = numHits
        myHdf5["/entry_1/result_1/maxPeakRow"].attrs["numEvents"] = numHits
        myHdf5["/entry_1/result_1/minPeakCol"].attrs["numEvents"] = numHits
        myHdf5["/entry_1/result_1/maxPeakCol"].attrs["numEvents"] = numHits
        myHdf5["/entry_1/result_1/maxRes"].attrs["numEvents"] = numHits
        myHdf5["/entry_1/result_1/likelihood"].attrs["numEvents"] = numHits
        myHdf5["/entry_1/result_1/timeToolDelay"].attrs["numEvents"] = numHits
        myHdf5["/entry_1/result_1/laserTimeZero"].attrs["numEvents"] = numHits
        myHdf5["/entry_1/result_1/laserTimeDelay"].attrs["numEvents"] = numHits
        myHdf5["/entry_1/result_1/laserTimePhaseLocked"].attrs["numEvents"] = numHits
        myHdf5["entry_1/instrument_1/source_1/energy"].attrs["numEvents"] = numHits
        myHdf5["entry_1/instrument_1/source_1/pulse_energy"].attrs["numEvents"] = numHits
        myHdf5["entry_1/instrument_1/source_1/pulse_width"].attrs["numEvents"] = numHits
        myHdf5["entry_1/instrument_1/detector_1/data"].attrs["numEvents"] = numHits
        myHdf5["entry_1/instrument_1/detector_1/distance"].attrs["numEvents"] = numHits
        myHdf5["entry_1/instrument_1/detector_1/x_pixel_size"].attrs["numEvents"] = numHits
        myHdf5["entry_1/instrument_1/detector_1/y_pixel_size"].attrs["numEvents"] = numHits
        myHdf5.flush()

    print( "Writing out cxi file")

    if '/status/findPeaks' in myHdf5: del myHdf5['/status/findPeaks']
    myHdf5['/status/findPeaks'] = 'success'
    myHdf5.flush()
    myHdf5.close()

    print("Closed cxi file")

    try:
        hitRate = numHits * 100. / numProcessed
        d = {"numHits": numHits, "hitRate(%)": round(hitRate,3), "fracDone(%)": 100., "projected": numHits}
        writeStatus(statusFname, d)
    except:
        pass

    # Save powder patterns
    if facility == 'LCLS':
        runTagStr = runStr
        if args.tag: runTagStr += "_"+args.tag
        fnameHits = args.outDir +"/"+ args.exp +"_"+ runTagStr + "_maxHits.npy"
        fnameMisses = args.outDir +"/"+ args.exp +"_"+ runTagStr + "_maxMisses.npy"
        fnameHitsTxt = args.outDir +"/"+ args.exp +"_"+ runTagStr + "_maxHits.txt"
        fnameMissesTxt = args.outDir +"/"+ args.exp +"_"+ runTagStr + "_maxMisses.txt"
        fnameHitsNatural = args.outDir +"/"+ args.exp +"_"+ runTagStr + "_maxHits_natural_shape.npy"
        fnameMissesNatural = args.outDir +"/"+ args.exp +"_"+ runTagStr + "_maxMisses_natural_shape.npy"

        # Save powder of hits
        if powderHits is not None:
            if powderHits.size == 2 * 185 * 388:  # cspad2x2
                # DAQ shape
                asData2x2 = two2x1ToData2x2(powderHits)
                np.save(fnameHits, asData2x2)
                np.savetxt(fnameHitsTxt, asData2x2.reshape((-1, asData2x2.shape[-1])), fmt='%0.18e')
                # Natural shape
                np.save(fnameHitsNatural, powderHits)
            else:
                np.save(fnameHits, powderHits)
                np.savetxt(fnameHitsTxt, powderHits.reshape((-1, powderHits.shape[-1])), fmt='%0.18e')
        # Save powder of misses
        if powderMisses is not None:
            if powderMisses.size == 2 * 185 * 388:  # cspad2x2
                # DAQ shape
                asData2x2 = two2x1ToData2x2(powderMisses)
                np.save(fnameMisses, asData2x2)
                np.savetxt(fnameMissesTxt, asData2x2.reshape((-1, asData2x2.shape[-1])), fmt='%0.18e')
                # Natural shape
                np.save(fnameMissesNatural, powderMisses)
            else:
                np.save(fnameMisses, powderMisses)
                np.savetxt(fnameMissesTxt, powderMisses.reshape((-1, powderMisses.shape[-1])), fmt='%0.18e')

    while nClients > 0:
        # Remove client if the run ended
        md = mpidata()
        md.recv()
        if md.small.endrun:
            nClients -= 1
        else:
            pass
