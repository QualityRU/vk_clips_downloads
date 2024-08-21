import asyncio
import concurrent.futures
import logging
import os
import random
import re
import sys
import traceback

import aiofiles
import vk_api
import yt_dlp
from colorama import Fore, init
from vk_api.exceptions import ApiError

init(autoreset=True)
logging.basicConfig(level=logging.INFO)


async def read_lines(file_path):
    async with aiofiles.open(file_path, mode='r') as file:
        return [line.strip() async for line in file]


async def read_proxies(file_path):
    proxies = []
    async with aiofiles.open(file_path, mode='r') as file:
        async for line in file:
            proxy = line.strip()
            if re.match(r'^\d+\.\d+\.\d+\.\d+:\d+:\w+:\w+$', proxy):
                ip, port, username, password = proxy.split(':')
                proxies.append(f'http://{username}:{password}@{ip}:{port}')
            else:
                print(Fore.RED + f'Неверный формат прокси: {proxy}')
    return proxies


def check_cache(cache_file, video_id):
    if not os.path.exists(cache_file):
        open(cache_file, 'w').close()
    with open(cache_file, mode='r') as cache:
        downloaded_ids = cache.read().splitlines()
    if video_id in downloaded_ids:
        print(
            Fore.MAGENTA
            + f'Клип закеширован и видимо уже был скачан: {video_id}'
        )
        return True
    with open(cache_file, mode='a') as cache:
        cache.write(video_id + '\n')


async def validate_token(token):
    try:
        vk_session = vk_api.VkApi(token=token)
        vk = vk_session.get_api()
        vk.account.getInfo()
        return token
    except (ApiError, Exception) as e:
        print(Fore.RED + f'Ошибка при проверке токена {token}: {e}')
        return None


async def get_valid_tokens(tokens):
    return [
        token
        for token in await asyncio.gather(
            *(validate_token(token) for token in tokens)
        )
        if token
    ]


async def get_group_id_and_name(vk, group_link):
    try:
        group_name = group_link.split('/')[-1]
        group_info = vk.groups.getById(group_id=group_name, fields='id')
        group_id = -group_info[0]['id']
        group_title = group_info[0]['name']
        return group_id, group_title
    except Exception as e:
        print(
            Fore.RED
            + f'Ошибка при получении ID и названия группы: {group_link}. Ошибка: {e}'
        )
        raise ValueError(
            f'Ошибка при получении ID и названия группы: {group_link}'
        )


def sanitize_filename(filename):
    return re.sub(r'[<>:"/\\|?*]', '_', filename)


async def download_video(video_url, save_dir, proxies, cache_file):
    video_id = re.search(r'id=(\d+)', video_url).group(1)
    ydl_opts = {
        'quiet': True,
        'outtmpl': os.path.join(save_dir, f'%(title)s_{video_id}.mp4'),
        'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]',
        'nocheckcertificate': True,
        'merge_output_format': 'mp4',
    }

    if proxies:
        ydl_opts['proxy'] = random.choice(proxies)

    def download():
        try:
            if check_cache(cache_file, video_id):
                return

            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info_dict = ydl.extract_info(video_url, download=False)
                title = info_dict.get('title', None)
                output_file = os.path.join(save_dir, f'{title}_{video_id}.mp4')

                if os.path.exists(output_file):
                    print(Fore.GREEN + f'Клип уже скачан: {output_file}')
                    return output_file

                ydl.download([video_url])
                print(Fore.CYAN + f'Клип успешно скачан: {output_file}')
                return output_file

        except Exception:
            print(Fore.RED + f'Ошибка скачивания:\n {traceback.format_exc()}')
            return None

    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, download)


async def fetch_video_urls(vk, group_id, group_title, count=200):
    videos = []
    offset = 0

    while True:
        sys.stdout.write(
            Fore.YELLOW
            + f'\rФормирую список ссылок всех клипов группы: {group_title}: {len(videos)}'
        )
        sys.stdout.flush()
        try:
            response = vk.video.get(
                owner_id=group_id, album_id=-6, offset=offset, count=count
            )
            items = response['items']
        except (ApiError, Exception) as e:
            logging.error(Fore.RED + f'Ошибка API VK: {e}')
            break

        if not items:
            break

        for video in items:
            video_url = video.get('player')
            if video_url and video_url not in videos:
                videos.append(video_url)

        offset += count

    return videos


async def get_clips(tokens, group_link, proxies):
    token = random.choice(await get_valid_tokens(tokens))
    vk_session = vk_api.VkApi(token=token)
    vk = vk_session.get_api()

    group_id, group_title = await get_group_id_and_name(vk, group_link)
    sanitized_group_title = sanitize_filename(group_title)
    save_dir = os.path.join('clips', f'Группа_{sanitized_group_title}')
    os.makedirs(save_dir, exist_ok=True)

    cache_file = os.path.join('cache', f'{sanitized_group_title}.txt')
    os.makedirs(os.path.dirname(cache_file), exist_ok=True)

    video_urls = await fetch_video_urls(vk, group_id, group_title)
    await asyncio.gather(
        *(
            download_video(video_url, save_dir, proxies, cache_file)
            for video_url in video_urls
        )
    )


async def main():
    tokens = await read_lines('tokens.txt')
    group_links = await read_lines('groups.txt')
    proxies = []  # Or await read_proxies('proxies.txt')

    valid_tokens = await get_valid_tokens(tokens)

    if not valid_tokens:
        print(Fore.RED + 'Нет действительных токенов.')
        return

    num_threads = min(len(group_links), len(valid_tokens))
    if num_threads == 0:
        print(Fore.RED + 'Нет ссылок на группы.')
        return

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=num_threads
    ) as executor:
        loop = asyncio.get_running_loop()
        tasks = [
            loop.run_in_executor(
                executor,
                lambda link: asyncio.run(
                    get_clips(valid_tokens, link, proxies)
                ),
                link,
            )
            for link in group_links
        ]
        await asyncio.gather(*tasks)


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except Exception as e:
        print(Fore.RED + f'Произошла ошибка: {e}')
