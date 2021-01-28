from setuptools import setup


setup(
    name='cldfbench_nts',
    py_modules=['cldfbench_nts'],
    include_package_data=True,
    zip_safe=False,
    entry_points={
        'cldfbench.dataset': [
            'nts=cldfbench_nts:Dataset',
        ]
    },
    install_requires=[
        'cldfbench>=1.3.0',
        'csvw>=1.8.1',
        'pycldf>=1.17.0',
        'clldutils>=3.6.0',
        'pylexibank>=2.8.2',
        'SQLAlchemy>=1.3.20',
        'ftfy>=5.8',
    ],
    extras_require={
        'test': [
            'pytest-cldf',
        ],
    },
)
