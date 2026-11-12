export function taskStatusStyle(status: string): {color: string; backgroundColor: string; fontWeight: string} {
  let colors = ['#374151', '#f1f5f9'];
  if (['approved', 'submitted', 'done', 'finished'].includes(status)) {
    colors = ['#166534', '#dcfce7'];
  } else if (['running', 'executing', 'ready'].includes(status)) {
    colors = ['#15803d', '#ecfccb'];
  } else if (['failed', 'broken', 'aborted', 'toabort'].includes(status)) {
    colors = ['#b91c1c', '#fee2e2'];
  } else if (['failures', 'exhausted', 'paused', 'holding'].includes(status)) {
    colors = ['#92400e', '#fef3c7'];
  } else if (['pending', 'waiting', 'submitting', 'registered', 'toretry', 'assigning', 'staging'].includes(status)) {
    colors = ['#1d4ed8', '#dbeafe'];
  } else if (status === 'archived') {
    colors = ['#86198f', '#fae8ff'];
  } else if (status === 'obsolete') {
    colors = ['#475569', '#e2e8f0'];
  }
  return {color: colors[0], backgroundColor: colors[1], fontWeight: '600'};
}
