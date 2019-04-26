from setuptools import setup


def read(file_name):
  with open(file_name) as f:
    return f.read()


setup(
    name='zerofs',
    version='0.1.1',
    description='Transparant filesystem backed by Backblaze B2 object store',
    long_description=read('README.md'),
    url='https://github.com/picklelo/zerofs',
    author='Nikhil Rao',
    author_email='nikhil@nikhilrao.me',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3'
    ],
    keywords='backblaze b2 zero fs filesystem',
    packages=['zerofs'],
    install_requires=['backblazeb2', 'fusepy']
)
