# PSEUDO CODE SOC MAX


# INPUTS
# start SOC (offset)
# drain (battery outflow) per activity
# maximum charging potential (maximum battery inflow) per activity
# Constraints: Maximum battery level, minimum battery level

# Preliminary operations
# delta = - drain + maximum charging potential
# maximum battery level start (first activity) = start SOC

# Pairwise cumsum implementation
# --> mathematically elegant solution does not seem to be feasible right now
