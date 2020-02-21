import io
from setuptools import setup
import monom

with io.open('README.md', 'rt') as f:
    readme = f.read()

setup(
    name='monom',
    version=monom.__version__,
    url='https://github.com/cymoo/monom',
    description='An object mapper for MongoDB with type hints.',
    long_description=readme,
    long_description_content_type='text/markdown',
    author='cymoo',
    author_email='wakenee@hotmail.com',
    license='MIT',
    platforms='any',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Operating System :: OS Independent',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Topic :: Database',
        'Topic :: Utilities',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    packages=['monom'],
    python_requires='>=3.6',
    install_requires=['pymongo>=3.7'],
    extras_require={'dev': ['pytest']},
)
