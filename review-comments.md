# general review comments

## remakrs with respect to future public releases

- we should consider switching to the standard 
  pip package structure at some point.
  The current setup seems ambigious as we
  have a source folder which contains not the source code
  which is in turn located in the scripts folder.
  This breaks with several conventions and could annoy users.
  
- before a public release we need to carefully comb out
  artefacts like the presentation folder. These artefacts
  either need a special license or they should be removed.
  
## remarks on global code structure or issues

- classes and scripts seem to be python modules. I hence
  expect an __init__.py in both. Is this the intention of 
  these folders?
  
- we are currently using print statements to
  inform the user. we may want to switch to the 
  standard python logging library for this purpose.
  This would empower the user to define its output
  based in his/her preferences.
  
- in general please avoid type declarations in variable/method
  names. This is really a problem down the road in maintenance
  and publication to externals. Since the human brain first reads
  and interprets the agenda setting is done long before
  any comment explaining the concept is being read. This
  can result in awkward discussions as the brain frequently
  sticks with its first impression and ignores conflicting
  information. case in point see review comment in flexEstimationManager 
  for variable dictBol.
  
- some names are difficult to understand. For example
  determinePurposeStartHour in tripDiaryManager
  makes much more sense to me if reformulated into
  determineStartHourPurpose so along natural language.
  We should discuss a renaming campaign.
  
- globalFunctions is the right structure to use. The name might
  be uninformative. Maybe tools would be a better name
  
- libLogging is unconventional in the python world.
  Python knows no libraries only modules and packages. Hence
  one would not use lib at all. This is more common in C
  and for compiled libraries which are linked at runtime.
  
- The purpose of sandboxFunctions is not clear to me
  It seemed redundant hence I skipped it for the review.
  
- why is there a yaml file in scripts? Could it not be where the other
  yaml files are placed?