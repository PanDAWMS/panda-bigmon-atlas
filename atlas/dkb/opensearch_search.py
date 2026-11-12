"""Read-only production DKB search API for the Angular search page."""
import logging

from opensearchpy.exceptions import RequestError, TransportError
from rest_framework.authentication import BasicAuthentication, SessionAuthentication
from rest_framework.decorators import api_view, authentication_classes, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response

from atlas.dkb.dkb2 import DKB_OS2_SEARCH

logger = logging.getLogger('prodtaskwebui')
TASK_LIMIT = 10000
PAGE_SIZES = (100, 250, 500, 1000)
SORT_FIELDS = {
    'task_id': 'taskid', 'pr_id': 'pr_id', 'total_events': 'total_events',
    'processed_events': 'processed_events',
    'taskname': 'taskname.keyword', 'status': 'status', 'campaign': 'campaign.keyword',
    'subcampaign': 'subcampaign.keyword', 'step_name': 'step_name.keyword',
    'ctag': 'ctag', 'project': 'project.keyword', 'run_number': 'run_number',
    'requested_events': 'requested_events', 'input_events': 'input_events',
}
TASK_FIELDS = [
    'taskid', 'taskname', 'pr_id', 'status', 'campaign', 'subcampaign',
    'step_name', 'ctag', 'project', 'total_events', 'processed_events',
    'requested_events', 'input_events', 'run_number',
]


def integer_param(params, name, default=None, minimum=0, maximum=None):
    value = params.get(name, '')
    if value == '':
        return default
    try:
        result = int(value)
    except (ValueError, TypeError):
        raise ValueError(f'{name} must be an integer.')
    if result < minimum or (maximum is not None and result > maximum):
        raise ValueError(f'{name} is outside the allowed range.')
    return result


def search_query(params):
    filters = []
    for name, field in (('step_name', 'step_name.keyword'), ('ctag', 'ctag')):
        value = params.get(name, '').strip()
        if value:
            filters.append({'term': {field: value}})
    for name, field in (('pr_id', 'pr_id'), ('task_id', 'taskid'), ('run_number', 'run_number')):
        lower = integer_param(params, name + '_min')
        upper = integer_param(params, name + '_max')
        if lower is not None and upper is not None and lower > upper:
            raise ValueError(f'{name} minimum must not exceed maximum.')
        bounds = {}
        if lower is not None:
            bounds['gte'] = lower
        if upper is not None:
            bounds['lte'] = upper
        if bounds:
            filters.append({'range': {field: bounds}})
    query = params.get('q', '').strip()
    must = [{'query_string': {'query': query, 'default_operator': 'AND',
                              'analyze_wildcard': True, 'lenient': True}}] if query else []
    return {'bool': {'must': must, 'filter': filters}}


def execute_search(body):
    connection = DKB_OS2_SEARCH['prod']
    response = connection['using'].search(index=connection['index'], body=body,
                                          request_timeout=60)
    if response.get('timed_out') or response.get('_shards', {}).get('failed', 0):
        raise TransportError(503, 'Incomplete OpenSearch response')
    return response


def search_error(error):
    if isinstance(error, ValueError):
        return Response({'detail': str(error)}, status=400)
    if isinstance(error, RequestError):
        return Response({'detail': 'Invalid search expression. Check the query syntax.'}, status=400)
    logger.exception('DKB OpenSearch search failed')
    return Response({'detail': 'DKB search is temporarily unavailable. Please retry.'}, status=503)


def matching_task_ids(params):
    """Use exactly the search page's filters when opening task management."""
    if not isinstance(params, dict):
        raise ValueError('OpenSearch filters must be an object.')
    response = execute_search({
        'query': search_query(params), 'size': TASK_LIMIT,
        '_source': ['taskid'], 'track_total_hits': True,
    })
    if response['hits']['total']['value'] >= TASK_LIMIT:
        raise ValueError('Manage tasks requires fewer than 10,000 matching tasks. Narrow your filters.')
    return [hit['_source']['taskid'] for hit in response['hits']['hits']]


@api_view(['GET'])
@authentication_classes([SessionAuthentication, BasicAuthentication])
@permission_classes([IsAuthenticated])
def opensearch_task(request, task_id):
    """Return the task's complete DKB document without querying production databases."""
    try:
        response = execute_search({
            'query': {'term': {'taskid': int(task_id)}}, 'size': 1,
        })
        hits = response['hits']['hits']
        if not hits:
            return Response({'detail': 'Task not found in DKB.'}, status=404)
        return Response(hits[0]['_source'])
    except (ValueError, TransportError) as error:
        return search_error(error)


@api_view(['GET'])
@authentication_classes([SessionAuthentication, BasicAuthentication])
@permission_classes([IsAuthenticated])
def opensearch_tasks(request):
    """Return one server-side page from the first 10,000 matching tasks."""
    try:
        query = search_query(request.query_params)
        page_size = integer_param(request.query_params, 'page_size', 100)
        if page_size not in PAGE_SIZES:
            raise ValueError('page_size must be 100, 250, 500 or 1000.')
        page = integer_param(request.query_params, 'page', 1, minimum=1,
                             maximum=TASK_LIMIT // page_size)
        sort = request.query_params.get('sort', 'task_id')
        direction = request.query_params.get('direction', 'desc')
        if sort not in SORT_FIELDS or direction not in ('asc', 'desc'):
            raise ValueError('Invalid sort field or direction.')
        ordering = [{SORT_FIELDS[sort]: {'order': direction, 'missing': '_last'}}]
        if sort != 'task_id':
            ordering.append({'taskid': {'order': 'desc'}})
        response = execute_search({
            'query': query, 'from': (page - 1) * page_size, 'size': page_size,
            'sort': ordering, '_source': TASK_FIELDS, 'track_total_hits': True,
        })
        total = response['hits']['total']['value']
        return Response({
            'tasks': [hit['_source'] for hit in response['hits']['hits']],
            'total': total, 'available': min(total, TASK_LIMIT), 'limit': TASK_LIMIT,
            'page': page, 'page_size': page_size,
        })
    except (ValueError, TransportError) as error:
        return search_error(error)


@api_view(['GET'])
@authentication_classes([SessionAuthentication, BasicAuthentication])
@permission_classes([IsAuthenticated])
def opensearch_summary(request):
    """Aggregate all matching tasks by subcampaign, independently of the table limit."""
    try:
        query = search_query(request.query_params)
        subcampaigns = []
        after = None
        while True:
            composite = {
                'size': 500,
                'sources': [{'subcampaign': {'terms': {
                    'field': 'subcampaign.keyword', 'missing_bucket': True,
                }}}],
            }
            if after is not None:
                composite['after'] = after
            response = execute_search({
                'query': query, 'size': 0, 'track_total_hits': True,
                'aggs': {'subcampaigns': {
                    'composite': composite,
                    'aggs': {'total_events': {'sum': {'field': 'total_events'}}},
                }},
            })
            aggregation = response['aggregations']['subcampaigns']
            for bucket in aggregation['buckets']:
                subcampaigns.append({
                    'subcampaign': bucket['key']['subcampaign'], 'tasks': bucket['doc_count'],
                    'total_events': bucket['total_events']['value'],
                })
            after = aggregation.get('after_key')
            if not after or not aggregation['buckets']:
                break
        return Response({
            'subcampaigns': subcampaigns,
            'total_tasks': sum(subcampaign['tasks'] for subcampaign in subcampaigns),
            'total_events': sum(subcampaign['total_events'] for subcampaign in subcampaigns),
        })
    except (ValueError, TransportError) as error:
        return search_error(error)
