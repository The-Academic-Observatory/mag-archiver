from setuptools import setup, find_packages

long_description = '''
mag-archiver is an Azure service that automatically archives Microsoft Academic Graph (MAG) releases so that they can 
be transferred to other cloud services.
'''

with open('requirements.txt') as f:
    install_requires = f.read().splitlines()

setup(
    name='mag-archiver',
    version='20.05.0',
    description='An Azure service that automatically archives Microsoft Academic Graph (MAG) releases.',
    long_description=long_description,
    license='Apache License Version 2.0',
    author='Curtin University',
    author_email='agent@observatory.academy',
    url='https://github.com/The-Academic-Observatory/mag-archiver',
    packages=find_packages(exclude=['tests']),
    download_url='https://github.com/The-Academic-Observatory/mag-archiver/v20.05.0.tar.gz',
    keywords=['MAG', 'Microsoft Academic Graph', 'science', 'data', 'workflows', 'academic institutes',
              'academic-observatory'],
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Topic :: Scientific/Engineering",
        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Utilities"
    ],
    python_requires='>=3.6.6'
)
