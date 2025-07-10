from setuptools import setup

setup(
    name='nps_intercom',
    version='0.3',
    install_requires=[
        'apache-beam[gcp]==2.64.0',
        'pandas>=1.3.5',
        'google-api-python-client>=2.70.0',
        'google-auth>=2.15.0',
        'google-auth-httplib2>=0.1.0',
        'google-auth-oauthlib>=0.7.1',
        'requests>=2.28.1'
    ],
)