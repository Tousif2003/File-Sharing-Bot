from Adarsh.vars import Var
from Adarsh.bot import StreamBot
from Adarsh.utils.human_readable import humanbytes
from Adarsh.utils.file_properties import get_file_ids
from Adarsh.server.exceptions import InvalidHash
import urllib.parse
import aiofiles
import logging
import aiohttp


async def render_page(id, secure_hash):
    file_data = await get_file_ids(StreamBot, int(Var.BIN_CHANNEL), int(id))
    if file_data.unique_id[:6] != secure_hash:
        logging.debug(f'link hash: {secure_hash} - {file_data.unique_id[:6]}')
        logging.debug(f"Invalid hash for message with - ID {id}")
        raise InvalidHash

    src = urllib.parse.urljoin(Var.URL, f'{secure_hash}{str(id)}')

    main_type = str(file_data.mime_type.split('/')[0].strip())

    if main_type == 'video':
        async with aiofiles.open('Adarsh/template/req.html') as r:
            heading = 'Watch {}'.format(file_data.file_name)
            tag = main_type
            template = (await r.read()).replace('tag', tag)

            # req.html me ab 12 %s hain → 12 values de rahe hain
            html = template % (
                heading,               # 1: <title>%s</title>
                file_data.file_name,   # 2: marquee wala %s
                src,                   # 3: <video src="%s">

                # VLC button
                src,                   # 4: VLC intent:%s
                file_data.file_name,   # 5: VLC S.title=%s

                # XPlayer button
                src,                   # 6: XPlayer intent:%s
                file_data.file_name,   # 7: XPlayer S.title=%s

                # KMPlayer button
                src,                   # 8: KMPlayer intent:%s
                file_data.file_name,   # 9: KMPlayer S.title=%s

                # Playit button
                src,                   # 10: Playit intent:%s
                file_data.file_name,   # 11: Playit S.title=%s

                # Download button
                src                    # 12: window.location.href='%s'
            )

    elif main_type == 'audio':
        async with aiofiles.open('Adarsh/template/req.html') as r:
            heading = 'Listen {}'.format(file_data.file_name)
            tag = main_type
            template = (await r.read()).replace('tag', tag)

            html = template % (
                heading,               # 1: <title>%s</title>
                file_data.file_name,   # 2: marquee
                src,                   # 3: <video src="%s"> (audio me bhi same template use ho raha)

                # VLC button
                src,                   # 4
                file_data.file_name,   # 5

                # XPlayer button
                src,                   # 6
                file_data.file_name,   # 7

                # KMPlayer button
                src,                   # 8
                file_data.file_name,   # 9

                # Playit button
                src,                   # 10
                file_data.file_name,   # 11

                # Download
                src                    # 12
            )

    else:
        async with aiofiles.open('Adarsh/template/dl.html') as r:
            async with aiohttp.ClientSession() as s:
                async with s.get(src) as u:
                    heading = 'Download {}'.format(file_data.file_name)
                    file_size = humanbytes(int(u.headers.get('Content-Length')))
                    html = (await r.read()) % (heading, file_data.file_name, src, file_size)

    return html
