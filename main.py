import asyncio
import concurrent.futures
import logging
import os
import random
import re
import subprocess
import sys
import traceback

import aiofiles
import vk_api
import yt_dlp
from colorama import Fore, init
from vk_api.exceptions import ApiError

init(autoreset=True)
logging.basicConfig(level=logging.INFO)


async def read_tokens(file_path):
    async with aiofiles.open(file_path, mode='r') as file:
        return [line.strip() async for line in file]


async def read_group_links(file_path):
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


async def validate_token(token):
    try:
        vk_session = vk_api.VkApi(token=token)
        vk = vk_session.get_api()
        vk.account.getInfo()
        return token
    except ApiError as e:
        print(Fore.RED + f'Ошибка API VK с токеном {token}: {e}')
        return None
    except Exception as e:
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
            + f'Не удается получить ID и название группы: {group_link}. Ошибка: {e}'
        )
        raise ValueError(
            f'Ошибка при получении ID и названия группы: {group_link}'
        )


def sanitize_filename(filename):
    invalid_chars = '<>:"/\\|?*'
    return ''.join(c if c not in invalid_chars else '_' for c in filename)


async def download_video(video_url, save_dir, proxies):
    video_id = re.search(r'id=(\d+)', video_url).group(1)

    ydl_opts = {
        'quiet': True,
        'outtmpl': os.path.join(save_dir, f'%(title)s_{video_id}.mp4'),
        'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]',
        'nocheckcertificate': True,
        'merge_output_format': 'mp4',
    }

    if proxies:
        proxy = random.choice(proxies)
        ydl_opts['proxy'] = proxy

    def download():
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            try:
                info_dict = ydl.extract_info(video_url, download=False)
                title = info_dict.get('title', None)
                output_file = f'{save_dir}/{title}_{video_id}.mp4'

                if os.path.exists(output_file):
                    print(Fore.GREEN + f'Видео уже скачано: {output_file}')
                    return

                ydl.download([video_url])
                print(Fore.CYAN + f'Видео успешно скачано: {output_file}')
                return output_file
            except Exception as e:
                print(
                    Fore.RED + f'Ошибка скачивания:\n {traceback.format_exc()}'
                )
                return

    loop = asyncio.get_running_loop()
    output_file = await loop.run_in_executor(None, download)

    # Проверка целостности видео закомментирована
    # try:
    #     if not await check_video_integrity(output_file):
    #         print(Fore.RED + f'Битое видео обнаружено: {output_file}')
    #         try:
    #             os.remove(output_file)
    #             print(Fore.RED + f'Битое видео удалено: {output_file}')
    #         except FileNotFoundError:
    #             print(Fore.RED + f'Ошибка: файл для удаления не найден: {output_file}')
    #         except Exception as e:
    #             print(Fore.RED + f'Ошибка при удалении файла: {e}')
    #     else:
    #         print(Fore.GREEN + f'Видео успешно проверено: {output_file}')
    # except Exception as e:
    #     print(Fore.RED + f'\nОшибка при проверке целостности видео {output_file}: {e}')


async def get_clips(tokens, group_link, executor, proxies):
    token = random.choice(await get_valid_tokens(tokens))
    vk_session = vk_api.VkApi(token=token)
    vk = vk_session.get_api()

    group_id, group_title = await get_group_id_and_name(vk, group_link)
    sanitized_group_title = sanitize_filename(group_title)
    save_dir = os.path.join('clips', f'Группа_{sanitized_group_title}')
    os.makedirs(save_dir, exist_ok=True)

    videos = []
    offset = 0
    count = 200

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
        except ApiError as e:
            logging.error(Fore.RED + f'Ошибка API VK: {e}')
            break
        except Exception as e:
            logging.error(Fore.RED + f'Произошла ошибка: {e}')
            break

        if not items:
            break

        for video in items:
            video_url = video.get('player')
            if video_url and video_url not in videos:
                videos.append(video_url)

        offset += count

    await asyncio.gather(
        *(download_video(video, save_dir, proxies) for video in videos)
    )


async def main():
    tokens = await read_tokens('tokens.txt')
    group_links = await read_group_links('groups.txt')
    # proxies = await read_proxies('proxies.txt')
    proxies = []

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
                    get_clips(valid_tokens, link, executor, proxies)
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
