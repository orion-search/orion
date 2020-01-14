from setuptools import setup
from setuptools import find_namespace_packages

exclude = ['tests*']
common_kwargs = dict(
    version='0.1.0',
    license='MIT',
    # install_requires=required,
    long_description=open('README.md').read(),
    url='https://github.com/kstathou/orion',
    author='Kostas Stathoulopoulos',
    author_email='k.stathou@gmail.com',
    maintainer='Kostas Stathoulopoulos',
    maintainer_email='k.stathou@gmail.com',
    classifiers=[
        'Development Status :: 1 - Planning',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.7',
        'Topic :: Scientific/Engineering :: Artificial Intelligence',
    ],
    python_requires='>3.6',
    include_package_data=False,
)

setup(name='orion',
      packages=find_namespace_packages(where='orion.*', exclude=exclude),
      **common_kwargs)
