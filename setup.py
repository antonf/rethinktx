from distutils.core import setup

setup(
    name='rethinktx',
    version='0.0.1',
    packages=[
        'rethinktx',
    ],
    license='Apache Software License',
    long_description=open('README.md').read(),
    install_requires=[
        'six>=1.9.0',
        'rethinkdb>=2.3.0',
    ],
)
