import os
from setuptools import setup, find_packages

VERSION = '0.2.0'

README = os.path.join(os.path.dirname(__file__), 'README.txt')
long_description = open(README).read() + '\n\n'

setup(name='bububa.SuperMario', 
    version=VERSION, 
    description=("A package that deals with web scrabe, " "from prof.syd.xu@gmail.com"),
    long_description=long_description,
    classifiers=[
    "Programming Language :: Python", 
    ("Topic :: Software Development :: Libraries :: Python Modules"), ],
    keywords='syd bububa crawler spider scraber web supermario', 
    author='Bububa', 
    author_email='prof.syd.xu@gmail.com', 
    url='http://syd.todayclose.com', 
    license='BSD', 
    packages=find_packages(), 
    namespace_packages=['bububa'],
    install_requires=['simplejson', 'BeautifulSoup', 'eventlet', 'PIL', 'pycurl', 'chardet', 'feedparser', 'mongokit', 'templatemaker', 'flickrapi', 'pyyaml', 'MySQLdb', 'dateutil'])