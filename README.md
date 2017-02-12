# Multiprocess
A very simple multi-process processing framework by python

# Design
* Single machine（node） execution
* Help python make full use of multi cpu-cores (GIL= Global Interpreter Lock)
* Keep Simple


# Setup / Install
    git clone https://github.com/jsonkey/multiprocess
    cd mutiprocess


# Man
* Help

![image](https://github.com/jsonkey/multiprocess/blob/master/help.JPG)

* Example & Test

![image](https://github.com/jsonkey/multiprocess/blob/master/example.JPG)

# Worker Description
    woker.py
    
    def func(lines):
            '''input:
                    lines = ['111111\n', '222222\n']
                output:
                    lines = ['aaaaaa\n', 'bbbbbb\n']
            '''
            rerurn lines


