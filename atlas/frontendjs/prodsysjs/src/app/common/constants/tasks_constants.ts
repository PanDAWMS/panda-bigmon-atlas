
export class DEFAULTS {
  public static readonly TIME_FORMAT = 'YYYY-MM-dd HH:mm';
}

export class PRODSYS_CONSTANTS {
  public static readonly PHYSICS_GROUPS = ['BPHY',
                                 'COSM',
                                 'DAPR',
                                 'EGAM',
                                 'EXOT',
                                 'FTAG',
                                 'HDBS',
                                 'HIGG',
                                 'HION',
                                 'IDET',
                                 'IDTR',
                                 'JETM',
                                 'LARG',
                                 'MCGN',
                                 'MDET',
                                 'MUON',
                                 'PHYS',
                                 'REPR',
                                 'SIMU',
                                 'SOFT',
                                 'STDM',
                                 'SUSY',
                                 'TAUP',
                                 'TCAL',
                                 'TDAQ',
                                 'TOPQ',
                                 'THLT',
                                 'TRIG',
                                 'VALI',
                                 'UPGR'];
}

export class TASKS_CONSTANTS {
  public static readonly TASKS_STATUS_ORDER = ['total', 'active', 'good', 'waiting', 'staging', 'registered', 'assigning',
    'submitting', 'ready', 'running', 'paused', 'exhausted', 'done', 'finished', 'toretry',
    'toabort', 'failed', 'broken', 'aborted', 'obsolete'];

  public static readonly BAD_TASKS_STATUS: string[] = ['failed', 'broken', 'aborted', 'toabort'];

  public static readonly STEPS_ORDER = ['total', 'Evgen',
                                         'Evgen Merge',
                                         'Simul',
                                         'Merge',
                                         'Digi',
                                         'Reco',
                                         'Rec Merge',
                                         'Atlfast',
                                         'Atlf Merge',
                                         'TAG',
                                         'Deriv',
                                         'Deriv Merge'];

  public static readonly TASKS_STATUS_GROUPING = {
    active: ['waiting', 'staging', 'registered', 'assigning', 'submitting', 'ready', 'running', 'paused',
      'exhausted', 'toretry', 'toabort'],
    good: ['waiting', 'staging', 'registered', 'assigning', 'submitting',
      'ready', 'running', 'paused', 'exhausted', 'done', 'finished', 'toretry']
  };

  public static readonly TASKS_PARAMS_FORM = {
  task_params_control: [
    {
      name: 'taskName',
      label: 'taskName',
      description: 'taskName',
      type: 'text',
      validators: {}
    },
    {
      name: 'uniqueTaskName',
      label: 'uniqueTaskName',
      description: 'uniqueTaskName',
      type: 'boolean',
      validators: {}
    },
    {
      name: 'vo',
      label: 'vo',
      description: 'vo',
      type: 'text',
      validators: {}
    },
    {
      name: 'architecture',
      label: 'architecture',
      description: 'architecture',
      type: 'text',
      validators: {}
    },
    {
      name: 'transUses',
      label: 'transUses',
      description: 'transUses',
      type: 'text',
      validators: {}
    },
    {
      name: 'transHome',
      label: 'transHome',
      description: 'transHome',
      type: 'text',
      validators: {}
    },
    {
      name: 'processingType',
      label: 'processingType',
      description: 'processingType',
      type: 'text',
      validators: {}
    },
    {
      name: 'prodSourceLabel',
      label: 'prodSourceLabel',
      description: 'prodSourceLabel',
      type: 'text',
      validators: {}
    },
    {
      name: 'site',
      label: 'site',
      description: 'site',
      type: 'text',
      validators: {}
    },
    {
      name: 'includedSite',
      label: 'includedSite',
      description: 'includedSite',
      type: 'text',
      validators: {}
    },
    {
      name: 'cliParams',
      label: 'cliParams',
      description: 'cliParams',
      type: 'text',
      validators: {}
    },
    {
      name: 'osInfo',
      label: 'osInfo',
      description: 'osInfo',
      type: 'text',
      validators: {}
    },
    {
      name: 'nMaxFilesPerJob',
      label: 'nMaxFilesPerJob',
      description: 'nMaxFilesPerJob',
      type: 'number',
      validators: {}
    },
    {
      name: 'respectSplitRule',
      label: 'respectSplitRule',
      description: 'respectSplitRule',
      type: 'boolean',
      validators: {}
    },
    {
      name: 'sourceURL',
      label: 'sourceURL',
      description: 'sourceURL',
      type: 'text',
      validators: {}
    },
    {
      name: 'dsForIN',
      label: 'dsForIN',
      description: 'dsForIN',
      type: 'text',
      validators: {}
    },
    {
      name: 'mergeOutput',
      label: 'mergeOutput',
      description: 'mergeOutput',
      type: 'boolean',
      validators: {}
    },
    {
      name: 'userName',
      label: 'userName',
      description: 'userName',
      type: 'text',
      validators: {}
    },
    {
      name: 'taskType',
      label: 'taskType',
      description: 'taskType',
      type: 'text',
      validators: {}
    },
    {
      name: 'taskPriority',
      label: 'taskPriority',
      description: 'taskPriority',
      type: 'number',
      required: true,
      validators: {required: true}
    },
    {
      name: 'nFilesPerJob',
      label: 'nFilesPerJob',
      description: 'nFilesPerJob',
      type: 'number',
      validators: {}
    },
    {
      name: 'fixedSandbox',
      label: 'fixedSandbox',
      description: 'fixedSandbox',
      type: 'text',
      validators: {}
    },
    {
      name: 'walltime',
      label: 'walltime',
      description: 'walltime',
      type: 'number',
      validators: {}
    },
    {
      name: 'coreCount',
      label: 'coreCount',
      description: 'coreCount',
      type: 'number',
      validators: {}
    },
    {
      name: 'noInput',
      label: 'noInput',
      description: 'noInput',
      type: 'boolean',
      validators: {}
    },
    {
      name: 'nEvents',
      label: 'nEvents',
      description: 'nEvents',
      type: 'number',
      validators: {}
    },
    {
      name: 'nEventsPerJob',
      label: 'nEventsPerJob',
      description: 'nEventsPerJob',
      type: 'number',
      validators: {}
    },
    {
      name: 'noEmail',
      label: 'noEmail',
      description: 'noEmail',
      type: 'boolean',
      validators: {}
    },
    {
      name: 'osMatching',
      label: 'osMatching',
      description: 'osMatching',
      type: 'boolean',
      validators: {}
    },
    {
      name: 'useRealNumEvents',
      label: 'useRealNumEvents',
      description: 'useRealNumEvents',
      type: 'boolean',
      validators: {}
    },
    {
      name: 'skipScout',
      label: 'skipScout',
      description: 'skipScout',
      type: 'boolean',
      validators: {}
    },
    {
      name: 'official',
      label: 'official',
      description: 'official',
      type: 'boolean',
      validators: {}
    },
    {
      name: 'respectLB',
      label: 'respectLB',
      description: 'respectLB',
      type: 'boolean',
      validators: {}
    },
    {
      name: 'maxAttempt',
      label: 'maxAttempt',
      description: 'maxAttempt',
      type: 'number',
      validators: {}
    },
    {
      name: 'useLocalIO',
      label: 'useLocalIO',
      description: 'useLocalIO',
      type: 'number',
      validators: {}
    },
    {
      name: 'workingGroup',
      label: 'workingGroup',
      description: 'workingGroup',
      type: 'text',
      validators: {}
    },
    {
      name: 'addNthFieldToLFN',
      label: 'addNthFieldToLFN',
      description: 'addNthFieldToLFN',
      type: 'text',
      validators: {}
    },
    {
      name: 'getNumEventsInMetadata',
      label: 'getNumEventsInMetadata',
      description: 'getNumEventsInMetadata',
      type: 'boolean',
      validators: {}
    },
    {
      name: 'baseRamCount',
      label: 'baseRamCount',
      description: 'baseRamCount',
      type: 'number',
      validators: {}
    },
    {
      name: 'campaign',
      label: 'campaign',
      description: 'campaign',
      type: 'text',
      validators: {}
    },
    {
      name: 'cloud',
      label: 'cloud',
      description: 'cloud',
      type: 'text',
      validators: {}
    },
    {
      name: 'cpuTimeUnit',
      label: 'cpuTimeUnit',
      description: 'cpuTimeUnit',
      type: 'text',
      validators: {}
    },
    {
      name: 'ipConnectivity',
      label: 'ipConnectivity',
      description: 'ipConnectivity',
      type: 'text',
      validators: {}
    },
    {
      name: 'maxFailure',
      label: 'maxFailure',
      description: 'maxFailure',
      type: 'number',
      validators: {}
    },
    {
      name: 'mergeCoreCount',
      label: 'mergeCoreCount',
      description: 'mergeCoreCount',
      type: 'number',
      validators: {}
    },
    {
      name: 'nGBPerJob',
      label: 'nGBPerJob',
      description: 'nGBPerJob',
      type: 'number',
      validators: {}
    },
    {
      name: 'noWaitParent',
      label: 'noWaitParent',
      description: 'noWaitParent',
      type: 'boolean',
      validators: {}
    },
    {
      name: 'ramCount',
      label: 'ramCount',
      description: 'ramCount',
      type: 'number',
      validators: {}
    },
    {
      name: 'ramUnit',
      label: 'ramUnit',
      description: 'ramUnit',
      type: 'text',
      validators: {}
    },
    {
      name: 'reqID',
      label: 'reqID',
      description: 'reqID',
      type: 'number',
      validators: {}
    },
    {
      name: 'scoutSuccessRate',
      label: 'scoutSuccessRate',
      description: 'scoutSuccessRate',
      type: 'number',
      validators: {}
    },
    {
      name: 'ticketID',
      label: 'ticketID',
      description: 'ticketID',
      type: 'text',
      validators: {}
    },
    {
      name: 'ticketSystemType',
      label: 'ticketSystemType',
      description: 'ticketSystemType',
      type: 'text',
      validators: {}
    },
    {
      name: 'transPath',
      label: 'transPath',
      description: 'transPath',
      type: 'text',
      validators: {}
    },
    {
      name: 'ramCountUnit',
      label: 'ramCountUnit',
      description: 'ramCountUnit',
      type: 'text',
      validators: {}
    },
    {
      name: 'container_name',
      label: 'container_name',
      description: 'container_name',
      type: 'text',
      validators: {}
    },
    {
      name: 'avoidVP',
      label: 'avoidVP',
      description: 'avoidVP',
      type: 'boolean',
      validators: {}
    },
    {
      name: 'runOnInstant',
      label: 'runOnInstant',
      description: 'runOnInstant',
      type: 'boolean',
      validators: {}
    },
    {
      name: 'nEventsPerFile',
      label: 'nEventsPerFile',
      description: 'nEventsPerFile',
      type: 'number',
      validators: {}
    },
    {
      name: 'parentTaskName',
      label: 'parentTaskName',
      description: 'parentTaskName',
      type: 'text',
      validators: {}
    },
    {
      name: 'nFiles',
      label: 'nFiles',
      description: 'nFiles',
      type: 'number',
      validators: {}
    },
    {
      name: 'disableAutoFinish',
      label: 'disableAutoFinish',
      description: 'disableAutoFinish',
      type: 'boolean',
      validators: {}
    },
    {
      name: 'failWhenGoalUnreached',
      label: 'failWhenGoalUnreached',
      description: 'failWhenGoalUnreached',
      type: 'boolean',
      validators: {}
    },
    {
      name: 'goal',
      label: 'goal',
      description: 'goal',
      type: 'text',
      validators: {}
    },
    {
      name: 'ttcrTimestamp',
      label: 'ttcrTimestamp',
      description: 'ttcrTimestamp',
      type: 'text',
      validators: {}
    },
    {
      name: 'useExhausted',
      label: 'useExhausted',
      description: 'useExhausted',
      type: 'boolean',
      validators: {}
    },
    {
      name: 'gshare',
      label: 'gshare',
      description: 'gshare',
      type: 'text',
      validators: {}
    },
    {
      name: 'ioIntensity',
      label: 'ioIntensity',
      description: 'ioIntensity',
      type: 'number',
      validators: {}
    },
    {
      name: 'ioIntensityUnit',
      label: 'ioIntensityUnit',
      description: 'ioIntensityUnit',
      type: 'text',
      validators: {}
    },
    {
      name: 'maxWalltime',
      label: 'maxWalltime',
      description: 'maxWalltime',
      type: 'number',
      validators: {}
    },
    {
      name: 'orderByLB',
      label: 'orderByLB',
      description: 'orderByLB',
      type: 'boolean',
      validators: {}
    },
    {
      name: 'outDiskCount',
      label: 'outDiskCount',
      description: 'outDiskCount',
      type: 'number',
      validators: {}
    },
    {
      name: 'outDiskUnit',
      label: 'outDiskUnit',
      description: 'outDiskUnit',
      type: 'text',
      validators: {}
    },
    {
      name: 'tgtMaxOutputForNG',
      label: 'tgtMaxOutputForNG',
      description: 'tgtMaxOutputForNG',
      type: 'number',
      validators: {}
    },
    {
      name: 'inputPreStaging',
      label: 'inputPreStaging',
      description: 'inputPreStaging',
      type: 'boolean',
      validators: {}
    },
    {
      name: 'noThrottle',
      label: 'noThrottle',
      description: 'noThrottle',
      type: 'boolean',
      validators: {}
    },
    {
      name: 'releasePerLB',
      label: 'releasePerLB',
      description: 'releasePerLB',
      type: 'boolean',
      validators: {}
    },
    {
      name: 'toStaging',
      label: 'toStaging',
      description: 'toStaging',
      type: 'boolean',
      validators: {}
    },
    {
      name: 'nGBPerMergeJob',
      label: 'nGBPerMergeJob',
      description: 'nGBPerMergeJob',
      type: 'text',
      validators: {}
    },
    {
      name: 'nEventsPerInputFile',
      label: 'nEventsPerInputFile',
      description: 'nEventsPerInputFile',
      type: 'number',
      validators: {}
    },
    {
      name: 'reuseSecOnDemand',
      label: 'reuseSecOnDemand',
      description: 'reuseSecOnDemand',
      type: 'boolean',
      validators: {}
    },
    {
      name: 'allowInputLAN',
      label: 'allowInputLAN',
      description: 'allowInputLAN',
      type: 'text',
      validators: {}
    },
    {
      name: 'cpuTime',
      label: 'cpuTime',
      description: 'cpuTime',
      type: 'number',
      validators: {}
    },
    {
      name: 'disableReassign',
      label: 'disableReassign',
      description: 'disableReassign',
      type: 'boolean',
      validators: {}
    },
    {
      name: 'esConvertible',
      label: 'esConvertible',
      description: 'esConvertible',
      type: 'boolean',
      validators: {}
    },
    {
      name: 'maxEventsPerJob',
      label: 'maxEventsPerJob',
      description: 'maxEventsPerJob',
      type: 'number',
      validators: {}
    },
    {
      name: 'minGranularity',
      label: 'minGranularity',
      description: 'minGranularity',
      type: 'number',
      validators: {}
    },
    {
      name: 'nucleus',
      label: 'nucleus',
      description: 'nucleus',
      type: 'text',
      validators: {}
    },
    {
      name: 'skipShortInput',
      label: 'skipShortInput',
      description: 'skipShortInput',
      type: 'boolean',
      validators: {}
    },
    {
      name: 'baseWalltime',
      label: 'baseWalltime',
      description: 'baseWalltime',
      type: 'number',
      validators: {}
    },
    {
      name: 'waitInput',
      label: 'waitInput',
      description: 'waitInput',
      type: 'number',
      validators: {}
    },
    {
      name: 'notDiscardEvents',
      label: 'notDiscardEvents',
      description: 'notDiscardEvents',
      type: 'boolean',
      validators: {}
    },
    {
      name: 'taskBrokerOnMaster',
      label: 'taskBrokerOnMaster',
      description: 'taskBrokerOnMaster',
      type: 'boolean',
      validators: {}
    },
    {
      name: 'noLoopingCheck',
      label: 'noLoopingCheck',
      description: 'noLoopingCheck',
      type: 'boolean',
      validators: {}
    },
    {
      name: 'tgtNumEventsPerJob',
      label: 'tgtNumEventsPerJob',
      description: 'tgtNumEventsPerJob',
      type: 'number',
      validators: {}
    },
    {
      name: 'countryGroup',
      label: 'countryGroup',
      description: 'countryGroup',
      type: 'text',
      validators: {}
    },
    {
      name: 'disableAutoRetry',
      label: 'disableAutoRetry',
      description: 'disableAutoRetry',
      type: 'number',
      validators: {}
    }
  ]
};
}
