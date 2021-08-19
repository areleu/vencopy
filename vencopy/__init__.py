# review: the declaration of the vencopy folder as a python module surprises me a bit.
#  I would expect only source code and python modules in this folder, as it declares itself as
#  a python module. Do we need this __init__.py file or can we just have one folder with the source code
#  which then behaves as a proper python module?
#  On the other hand, it seems as if an __init__.py file is both missing in the scripts and classes folders as these are
#  valid python modules.