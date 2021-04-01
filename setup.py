import setuptools


setuptools.setup(
    name='bulq-transform-add-column',
    version='0.0.1',
    install_requires=[],
    packages=setuptools.find_packages(),
    entry_points={
        'bulq.plugins.transform': [
            f'add_column = bulq_transform_add_column',
        ],
    }
)

