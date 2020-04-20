import re
from setuptools import setup

short_description = " Wrapper around Minio with an interface for simple storage of python objects."
with open('README.md') as f:
    long_description = f.read()


with open('storage.py') as f:
    version = next(
        re.finditer(
            r'\n__version__ *= *[\'\"]([0-9\.]+)[\'\"]',
            f.read(),
        )
    ).groups()[0]


setup(
    name='storage',
    version=version,
    author='Dylan Gregersen',
    author_email='an.email0101@gmail.com',
    url='https://github.com/earthastronaut/storage',
    license='MIT',
    description=short_description,
    long_description=long_description,
    py_modules=['storage'],
    python_requires='>=3.6',
    install_requires=[
        "minio"
    ],
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
    ]
)
