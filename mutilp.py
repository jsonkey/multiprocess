#!/usr/bin/env python
# vim: tabstop=4 shiftwidth=4 softtabstop=4
# baoyongcheng@jd.com
#

"""
A very simple multi-process processing framework by python
"""
import os
import sys
import time
import multiprocessing
from optparse import OptionParser


parser = OptionParser()
parser.add_option("-q", "--quiet", action="store_false", dest="verbose", default=True,
                  help="don't print status messages to stdout")
parser.add_option("-C", "--cpus", dest="cpus", default=multiprocessing.cpu_count()-2, type="int",
                  help="how many worker need be forks, one worker one cpu-core")
parser.add_option("-I", "--infile", dest="infile", type="string",
                  help="Need to be processed by the original data file, lines format")
parser.add_option("-O", "--outfile", dest="outfile", type="string",
                  help="file path, which Save the final result")
parser.add_option("-L", "--log-file", dest="log_path", type="string",
                  help="the path, which for save execute cmd (stdout, stderr)")
parser.add_option("-W", "--worker", dest="worker", type="string",
                  help="real deal with execute string, we call worker.func(lines) return lines; lines=['111111\n', '22222\n']")
parser.add_option("-B", "--lines", dest="lines", type="int",
                  help="len(lines); means how many lines input to worker.func")

(CONF, ARGS) = parser.parse_args()

g_log_path = CONF.log_path or '/tmp/abc/'

C = CONF.cpus
INPUT = multiprocessing.Queue(1000000)
OUTPUT= multiprocessing.Queue(1000000)
CHECKOUT=multiprocessing.Queue(C)
V = multiprocessing.Value('i', 0)
R = multiprocessing.RLock()

buffer = CONF.lines or 10000

input_file = CONF.infile
assert os.path.isfile(input_file), "%s is not exists, pls input source files"%input_file

output_file= CONF.outfile
assert not os.path.isfile(output_file), "%s is exists, pls save and del first"%output_file
output_dir = os.path.dirname(output_file)
os.path.isdir(output_dir) or os.makedirs(output_dir)

worker = CONF.worker
assert worker and os.path.isfile(worker), "pls make sure worker.py"
sys.path.append(os.path.abspath(os.curdir))
sys.path.append(os.path.dirname(worker))

from worker import func

def src2queue(infile, INPUT, V):
	with open(infile) as fd:
		lines = []
		for e in fd:
			lines.append(e)
			if len(lines) >= buffer:
				INPUT.put(lines)
				lines=[]
		V.value = 1
	p=multiprocessing.current_process()
	print("reader   p.name: " + p.name + "\tp.pid: " + str(p.pid))


def queue2dst(outfile, OUTPUT, CHECKOUT, V, C, R):
	fd = open(outfile, 'a+')
	try:
		R.acquire()
		while True:
			try:
				lines = OUTPUT.get_nowait()
			except:
				lines = None
			if lines:
				fd.write(''.join(lines))
			elif V.value and C == CHECKOUT.qsize() and 0 == OUTPUT.qsize():
				break
	finally:
		fd.close()
		R.release()
	p=multiprocessing.current_process()
	print("writer   p.name: " + p.name + "\tp.pid: " + str(p.pid))


def worker(INPUT, OUTPUT, CHECKOUT, V):
	while True:
		try:
			inlines = INPUT.get_nowait()
		except:
			inlines = None
		if inlines:
			outlines=func(inlines)
			if outlines:
				OUTPUT.put(outlines)
		elif V.value and 0 == INPUT.qsize():
			CHECKOUT.put(multiprocessing.current_process().name)
			break
	p=multiprocessing.current_process()
	print("worker   p.name: " + p.name + "\tp.pid: " + str(p.pid))


def main():
	p_list = []
	p1 = multiprocessing.Process(target = src2queue, args = (input_file, INPUT, V))
	p1.daemon = True
	p1.start()
	p_list.append(p1)
	p2 = multiprocessing.Process(target = queue2dst, args = (output_file, OUTPUT, CHECKOUT, V, C, R))
	p2.daemon = True
	p2.start()
	p_list.append(p2)
	for e in range(C):
		p = multiprocessing.Process(target = worker, args = (INPUT, OUTPUT, CHECKOUT, V))
		p.daemon = True
		p.start()
		p_list.append(p)

	print("The number of CPU is:" + str(multiprocessing.cpu_count()))
	print "Start !!!!!!!!!!!!!!!!!"
	for p in p_list:
		print("child    p.name: " + p.name + "\tp.pid: " + str(p.pid))
	print "Doing !!!!!!!!!!!!!!!!!"
	R.acquire()
	time.sleep(3)
	print "END !!!!!!!!!!!!!!!!!"


if __name__ == '__main__':
	main()
