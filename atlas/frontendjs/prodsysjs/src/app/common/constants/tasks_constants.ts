

export class TASKS_CONSTANTS {
  public static readonly TASKS_STATUS_ORDER = ['total', 'active', 'good', 'waiting', 'staging', 'registered', 'assigning',
    'submitting', 'ready', 'running', 'paused', 'exhausted', 'done', 'finished', 'toretry',
    'toabort', 'failed', 'broken', 'aborted', 'obsolete'];

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

}
