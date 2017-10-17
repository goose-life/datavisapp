import setuptools

setuptools.setup(
    name="datavisapp",
    version="0.1.0",
    url="https://github.com/edithvee/datavisapp",

    author="edithvee",
    author_email="32566441+edithvee@users.noreply.github.com",

    description="a data visualisation app _for science_",
    long_description=open('README.md').read(),

    packages=setuptools.find_packages(),

    install_requires=[],

    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
    ],

    entry_points={
        'console_scripts': [
            'create_db = datavisapp.create_db:cli',
        ],
    },

)
