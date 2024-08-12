import asyncio
import os
import re
import sys

import aiofiles
import vk_api
import yt_dlp
from vk_api.exceptions import ApiError


async def read_tokens(file_path):
    tokens = []
    async with aiofiles.open(file_path, mode='r') as file:
        async for line in file:
            tokens.append(line.strip())
    return tokens


async def read_group_links(file_path):
    links = []
    async with aiofiles.open(file_path, mode='r') as file:
        async for line in file:
            links.append(line.strip())
    return links


def extract_group_id(group_link):
    match = re.search(r'(?:public|club)(\d+)', group_link)
    if match:
        return -int(match.group(1))
    else:
        raise ValueError(
            f'Не удается извлечь ID группы из ссылки: {group_link}'
        )


async def get_clips(token, group_id):
    videos = []
    offset = 0
    count = 200

    try:
        vk_session = vk_api.VkApi(token=token)
        vk = vk_session.get_api()
        while True:
            sys.stdout.write(
                f'\rФормирую список ссылок всех клипов группы: https://vk.com/public{group_id}: {len(videos)}'
            )
            sys.stdout.flush()
            response = vk.video.get(
                owner_id=group_id, album_id=-6, offset=offset, count=count
            )
            items = response['items']
            if not items:
                break

            for video in items:
                if video.get('player') not in videos:
                    videos.append(video.get('player'))

            offset += count

        for video in videos:
            await download_video(video)

    except ApiError as e:
        print(f'Ошибка VK API: {e}')
    except Exception as e:
        print(f'Ошибка при получении видео: {e}')


async def download_video(video_url, save_dir='videos', proxy=None):
    try:
        os.makedirs(save_dir, exist_ok=True)
        video_id = re.search(r'id=(\d+)', video_url).group(1)

        ydl_opts = {
            'outtmpl': os.path.join(save_dir, f'%(title)s_{video_id}.mp4'),
            'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]',
            'proxy': proxy,
            'nocheckcertificate': True,
            'merge_output_format': 'mp4',
        }

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([video_url])

        print(f'Видео загружено с {video_url} в {save_dir}')
    except Exception as e:
        print(f'Ошибка при загрузке видео: {e}')


async def main():
    tokens = await read_tokens('tokens.txt')
    group_links = await read_group_links('groups.txt')

    if len(tokens) != len(group_links):
        print('Количество токенов и ссылок на группы не совпадает.')
        return

    for token, group_link in zip(tokens, group_links):
        group_id = extract_group_id(group_link)
        await get_clips(token, group_id)

    proxy = 'http://your.proxy.server:port'

    tasks = []
    for token in tokens:
        for group_link in group_links:
            group_id = extract_group_id(group_link)
            tasks.append(get_clips(token, group_id))

    await asyncio.gather(*tasks)


if __name__ == '__main__':
    asyncio.run(main())
