#define NUM_EVENTS 7

static int _profile_events[NUM_EVENTS] = { PAPI_L1_TCM,  PAPI_L2_TCM,  PAPI_L3_TCM,  PAPI_L1_ICM,  PAPI_L1_DCM,  PAPI_L2_DCA, PAPI_L3_TCA };

//Cab L1
//static int _profile_events[NUM_EVENTS] = { PAPI_L1_DCM, PAPI_L1_LDM, PAPI_L1_STM };
//static int _profile_events[NUM_EVENTS] = { PAPI_L1_TCM, PAPI_L2_TCM, PAPI_L3_TCM };
//static int _profile_events[NUM_EVENTS] = { PAPI_L1_LDM, PAPI_L1_STM, PAPI_L1_TCM, PAPI_L1_DCM, PAPI_L1_ICM };

//static int _profile_events[NUM_EVENTS] = { PAPI_L2_DCM, PAPI_L2_ICM, PAPI_L2_TCM, PAPI_L2_TCA, PAPI_L2_DCA, PAPI_L2_ICA };
//static int _profile_events[NUM_EVENTS] = { PAPI_L3_TCM, PAPI_L3_TCA, PAPI_L3_TCR, PAPI_L3_TCW };
//static int _profile_events[NUM_EVENTS] = { PAPI_L2_DCM, PAPI_L2_ICM, PAPI_L2_TCM, PAPI_L2_TCA, PAPI_L2_DCA};

//Hera debugging
//static int _profile_events[NUM_EVENTS] = { PAPI_L2_TCM, PAPI_L2_DCA, PAPI_L2_DCH, PAPI_L2_DCM };


