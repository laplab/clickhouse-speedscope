import argparse
from urllib.parse import urlencode
import sys

from aiohttp import web
from aioch import Client
import aiohttp_cors


async def query(request, ch_host, ch_port):
    query_id = request.query.get('query_id', None)
    if not query_id:
        raise web.HTTPBadRequest(text='Expected query id')

    client = Client(ch_host, port=ch_port)
    await client.execute('SET allow_introspection_functions=1;')

    try:
        rows = await client.execute(
            '''SELECT
                arrayStringConcat(arrayReverse(arrayMap(x -> demangle(addressToSymbol(x)), trace)), ';') AS stack,
                count() AS samples
            FROM system.trace_log
            WHERE query_id = %(query_id)s
            GROUP BY trace''',
            {'query_id': query_id},
        )
    except:
        raise web.HTTPBadRequest(text='Invalid query id')

    if len(rows) == 0:
        raise web.HTTPBadRequest(text='Invalid query id')

    result = '\n'.join([f'{row[0]} {row[1]}' for row in rows])
    return web.Response(text=result)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='SpeedScope proxy for ClickHouse')
    parser.add_argument('--ch-host', metavar='CLICKHOUSE_HOST', help='ClickHouse host', default='localhost')
    parser.add_argument('--ch-port', metavar='CLICKHOUSE_PORT', type=int, help='ClickHouse port', default=9000)
    parser.add_argument('--proxy-host', metavar='PROXY_HOST', help='Host for proxy endpoints', default='localhost')
    parser.add_argument('--proxy-port', metavar='PROXY_PORT', type=int, help='Proxy port', default=8080)
    parser.add_argument('--query-id', metavar='QUERY_ID', help='Print SpeedScope url for query id')
    args = parser.parse_args()

    if args.query_id:
        proxy_url = f'http://{args.proxy_host}:{args.proxy_port}/query?' + urlencode({'query_id': args.query_id})
        speedscope_url = 'https://www.speedscope.app/#' + urlencode({'profileURL': proxy_url})
        print(speedscope_url)
        sys.exit(0)

    app = web.Application()
    cors = aiohttp_cors.setup(app)
    resource = cors.add(app.router.add_resource('/query'))
    route = resource.add_route('GET', lambda r: query(r, args.ch_host, args.ch_port))
    cors.add(
        route,
        {
            '*': aiohttp_cors.ResourceOptions(
                allow_credentials=True,
                expose_headers=['Access-Control-Allow-Origin'],
                allow_headers=['Access-Control-Allow-Origin'],
                max_age=3600,
                allow_methods=['GET'],
            ),
        },
    )
    web.run_app(app, host=args.proxy_host, port=args.proxy_port)
