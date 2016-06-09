#!/usr/bin/env python

import glob
import luigi
import os
import subprocess
import re
import gzip

class mysection(luigi.Config):
    projectFolder = luigi.Parameter(default="./")
    config        = luigi.Parameter()
    cores         = luigi.IntParameter(default=1)
    regexText     = luigi.Parameter()


class Merge(luigi.WrapperTask):
    """
    calls mergeFQ on all samples
    """
    def requires(self):
        for k in range(1):
            yield MergeFQ(sampleNumber = k)

class MergeFQ(luigi.Task):
    """
    This will concatenate READ1 + READ2 + singletons back into 1 gz file
    """
    sampleNumber = luigi.IntParameter()

    def requires(self):
        return Fastqc()

    def run(self):
        print "====== Running Merging Individual fastQ ======"

        samples = filter(lambda f: os.path.isdir(f), glob.glob(mysection().projectFolder+"/data/trimmed/*"))
        with self.output().open('w') as logFile:
            for samplePath in samples:
                sample = samplePath.split("/")[-1]
                logFile.write(sample+"\n")
                fqs = os.listdir(samplePath+"/split-adapter-quality-trimmed/")
                print fqs
                combined = samplePath+ "/"+sample+"-combined.fastq.gz"
                with gzip.open(combined, 'w') as combinedFile:
                #problem read1 read2 singleton
                    for fq in fqs:
                        fq = samplePath + "/split-adapter-quality-trimmed/" + fq
                        print fq
                        with gzip.open(fq, 'r') as fqfile:
                            for line in fqfile:
                                theMatch = re.search(str(mysection().regexText), line)
                                if theMatch:
                                    pre  = theMatch.group(1)
                                    post = theMatch.group(2)
                                    combinedFile.write(pre+"/"+post+"\n")
                                else:
                                    combinedFile.write(line)

    def output(self):
        return luigi.LocalTarget(path="%s/logs/merge.logs" % mysection().projectFolder)


class Fastqc(luigi.Task):
    """
    Runs fastQC on the samples and outputs into the folder structure built by illumiprocessor
    """
    def requires(self):
        return Trim()
    def run(self):
        print "====== Running FastQC ======"
        samples = filter(lambda f: os.path.isdir(f), glob.glob(mysection().projectFolder+"/data/trimmed/*"))
        with self.output().open('w') as logFile:
            for sample in samples:
                fq = os.listdir(sample+'/raw-reads')
                if not os.path.exists(sample+"/fastqc"):
                    os.mkdir(sample+"/fastqc")
                cmd = '''fastqc -o {sample}/fastqc {sample}/raw-reads/{read1} {sample}/raw-reads/{read2}'''.format(sample=sample, read1=fq[0], read2=fq[1])
                logFile.write(cmd)
                out = subprocess.check_output(cmd, shell=True)
                logFile.write(out)
                logFile.write("Sample: " + sample)
    def output(self):
        return luigi.LocalTarget("%s/logs/fastqc.logs" % mysection().projectFolder)

class Trim(luigi.Task):
    """
    Runs Illumiprocessor - trimmomatic
    """
    def run(self):
        print "====== Running Trimmomatic ======"
        if not os.path.exists(mysection().projectFolder+"/out"):
            os.mkdir(mysection().projectFolder+"/out")
        if not os.path.exists(mysection().projectFolder+"/logs"):
            os.mkdir(mysection().projectFolder+"/logs")
        cmd = '''illumiprocessor  --input {proj}/data/fastqFiles --output {proj}/data/trimmed --config {config} --trimmomatic /export2/home/uesu/local/anaconda/jar/trimmomatic.jar --cores {cores} --log-path {proj}/logs'''.format(proj=mysection().projectFolder, config=mysection().config, cores=str(mysection().cores))
        out = subprocess.check_output(cmd, shell=True)
        print "====== Process Info ======"
        print out
        print "=========================="
    def output(self):
        return luigi.LocalTarget("%s/logs/illumiprocessor.log" % mysection().projectFolder)
