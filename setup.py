from codecs import open
from os import path

from setuptools import setup

basedir = path.abspath(path.dirname(__file__))

with open(path.join(basedir, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='simple-amqp-rpc',
    version='0.2.0',
    description='Simple AMQP RPC lib',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/rudineirk/py-simple-amqp-rpc',
    author='Rudinei Goi Roecker',
    author_email='rudinei.roecker@gmail.com',
    license='MIT',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries :: Application Frameworks',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
    ],
    keywords='simple amqp rpc',
    packages=['simple_amqp_rpc', 'simple_amqp_rpc.base'],
    install_requires=[
        'simple_amqp>=0.2.3',
        'msgpack',
    ],
    extras_require={
        'asyncio': [
            'aio-pika',
        ],
        'gevent': [
            'gevent',
        ]
    },
)
