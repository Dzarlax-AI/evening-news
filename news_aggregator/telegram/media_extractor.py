"""Media extraction from Telegram messages."""

import re
from typing import List, Dict, Any, Optional, Set
from bs4 import BeautifulSoup


class MediaExtractor:
    """Extract media files and information from Telegram message elements."""

    def __init__(self):
        """Initialize MediaExtractor with enhanced selectors for all media types."""
        # Comprehensive media selectors for all Telegram media types
        self.media_selectors = {
            'image': [
                '.media img',                           # Media container images
                '.photo img',                           # Photo-specific images
                '.attachment img',                      # Attachment images
                '.message_media img',                   # Message media
                '.tgme_widget_message_photo img',       # Telegram preview photos
                '.tgme_widget_message_photo',           # Photo containers
                '.message_media_photo img',             # Photo wrapper images
                '.js-message_photo img',                # JS photo elements
            ],
            'video': [
                '.tgme_widget_message_video',           # Video containers
                '.tgme_widget_message_video_player',    # Video players
                '.message_media_video',                 # Video wrapper
                '.js-message_video',                    # JS video elements
                'video',                                # Direct video elements
                '.video-thumb img',                     # Video thumbnails
                '.message_video',                       # Video messages
                '.video_note',                          # Video notes (circles)
                '.round_video',                         # Round video messages
            ],
            'audio': [
                '.tgme_widget_message_voice',           # Voice messages
                '.tgme_widget_message_audio',           # Audio files
                '.message_media_audio',                 # Audio wrapper
                '.js-message_audio',                    # JS audio elements
                '.audio_file',                          # Audio files
                '.voice_message',                       # Voice messages
                '.voice_note',                          # Voice notes
                'audio',                                # Direct audio elements
            ],
            'sticker': [
                '.tgme_widget_message_sticker',         # Stickers
                '.sticker_wrap',                        # Sticker wrapper
                '.animated_sticker',                    # Animated stickers
                '.message_sticker',                     # Sticker messages
                '.sticker img',                         # Sticker images
            ],
            'document': [
                '.tgme_widget_message_document',        # Document containers
                '.document-thumb img',                  # Document thumbnails
                '.message_media_document',              # Document wrapper
                '.js-message_document',                 # JS document elements
                '.document_wrap',                       # Document wrapper
                '.document_file',                       # Document files
            ],
            'gif': [
                '.tgme_widget_message_gif',             # GIF containers
                '.message_media_gif',                   # GIF wrapper
                '.js-message_gif',                      # JS GIF elements
                '.gif_wrap',                            # GIF wrapper
                '.animated_gif',                        # Animated GIFs
            ],
            'poll': [
                '.tgme_widget_message_poll',            # Polls
                '.message_media_poll',                  # Poll wrapper
                '.poll_wrap',                           # Poll wrapper
            ],
            'location': [
                '.tgme_widget_message_location',        # Location shares
                '.message_media_location',              # Location wrapper
                '.location_wrap',                       # Location wrapper
                '.location_point',                      # Location points
            ],
            'contact': [
                '.tgme_widget_message_contact',         # Contact shares
                '.message_media_contact',               # Contact wrapper
                '.contact_wrap',                        # Contact wrapper
            ]
        }

        # Avatar/profile selectors to exclude
        self.avatar_selectors = [
            '.tgme_widget_message_user_photo img',       # User profile photos
            '.tgme_widget_message_owner_photo img',      # Channel owner photos
            '.tgme_widget_message_forwarded_from img',   # Forwarded-from channel icon
            '.message_author_photo img',                 # Author photos
            '.avatar img',                               # Generic avatars
            '.profile img',                              # Profile images
            '.channel_photo img',                        # Channel photos
            '.emoji img',                                # Regular emoji images
            '.tg-emoji img',                             # Custom emoji sticker images
            'tg-emoji img',                              # Custom emoji (tag selector)
        ]

        # Container selectors whose contents should be excluded entirely
        self.excluded_containers = [
            '.tgme_widget_message_forwarded_from',  # Forwarded-from block (has channel avatar)
            '.tgme_widget_message_reply',           # Quoted reply block
        ]

    def extract_media_files(self, message_div) -> List[Dict[str, Any]]:
        """Extract all media files from message div with enhanced media support, excluding channel avatars."""
        media_files = []

        # First, collect avatar URLs to exclude them
        avatar_urls = self._collect_avatar_urls(message_div)

        # Extract all types of media with enhanced support
        media_files.extend(self._extract_images(message_div, avatar_urls))
        media_files.extend(self._extract_videos(message_div, avatar_urls))
        media_files.extend(self._extract_audio(message_div, avatar_urls))
        media_files.extend(self._extract_stickers(message_div, avatar_urls))
        media_files.extend(self._extract_gifs(message_div, avatar_urls))
        media_files.extend(self._extract_documents(message_div, avatar_urls))
        media_files.extend(self._extract_polls(message_div))
        media_files.extend(self._extract_locations(message_div))
        media_files.extend(self._extract_contacts(message_div))
        media_files.extend(self._extract_background_images(message_div, avatar_urls))

        # Remove duplicates based on URL
        return self._deduplicate_media(media_files)

    def extract_media_info(self, message_div) -> Dict[str, Any]:
        """Extract comprehensive media information from message."""
        media_info = {
            'video_url': None,
            'document_url': None,
            'audio_url': None,
            'sticker_url': None,
            'poll_data': None,
            'location_data': None,
            'media_type': None,
            'duration': None,
            'file_size': None,
            'file_name': None
        }

        # Extract different types of media info
        self._extract_video_info(message_div, media_info)
        self._extract_document_info(message_div, media_info)
        self._extract_audio_info(message_div, media_info)
        self._extract_sticker_info(message_div, media_info)
        self._extract_poll_info(message_div, media_info)
        self._extract_location_info(message_div, media_info)

        return media_info

    def extract_image_url(self, message_div) -> Optional[str]:
        """Extract single image URL for backward compatibility."""
        media_files = self.extract_media_files(message_div)

        # Find first image in media files
        for media in media_files:
            if media.get('type') == 'image':
                return media.get('url')

        return None

    # Avatar/profile containers whose background-images should be excluded
    AVATAR_BG_SELECTORS = [
        '.tgme_widget_message_user_photo',
        '.tgme_widget_message_owner_photo',
        '.message_author_photo',
        '.avatar',
        '.profile',
        '.channel_photo',
    ]

    def _collect_avatar_urls(self, message_div) -> Set[str]:
        """Collect avatar/icon URLs to exclude from media extraction.

        Includes:
        - Explicit avatar img selectors
        - Background-images from avatar containers (user photo, owner photo, etc.)
        - All images inside excluded containers (forwarded-from, reply quote, etc.)
        - Background-images inside excluded containers
        """
        avatar_urls = set()

        # Collect from explicit avatar selectors (img tags)
        for selector in self.avatar_selectors:
            for img in message_div.select(selector):
                src = img.get('src') or img.get('data-src') or img.get('data-lazy-src')
                if src:
                    avatar_urls.add(src)

        # Collect background-images from avatar containers
        # (Telegram uses background-image on <i> elements for channel/user photos)
        for selector in self.AVATAR_BG_SELECTORS:
            for container in message_div.select(selector):
                # Check the container itself and all children for background-image
                for el in [container] + container.select('[style*="background-image"]'):
                    style = el.get('style', '')
                    if 'background-image' in style:
                        for match in re.findall(r'background-image:\s*url\(["\']?(.*?)["\']?\)', style):
                            if match:
                                avatar_urls.add(match)

        # Collect all images from excluded container blocks
        for container_selector in self.excluded_containers:
            for container in message_div.select(container_selector):
                # img src attributes
                for img in container.select('img'):
                    src = img.get('src') or img.get('data-src') or img.get('data-lazy-src')
                    if src:
                        avatar_urls.add(src)
                # background-image in style attributes
                for el in container.select('[style*="background-image"]'):
                    style = el.get('style', '')
                    for match in re.findall(r'background-image:\s*url\(["\']?(.*?)["\']?\)', style):
                        if match:
                            avatar_urls.add(match)

        return avatar_urls

    def _extract_images(self, message_div, avatar_urls: Set[str]) -> List[Dict[str, Any]]:
        """Extract image media files."""
        images = []
        for selector in self.media_selectors['image']:
            imgs = message_div.select(selector)
            for img in imgs:
                src = img.get('src') or img.get('data-src') or img.get('data-lazy-src')
                if src and src not in avatar_urls:
                    normalized_url = self._normalize_image_url(src)
                    if normalized_url and self._is_content_image(normalized_url):
                        images.append({
                            'url': normalized_url,
                            'type': 'image',
                            'thumbnail': normalized_url  # For images, thumbnail is the same as URL
                        })
        return images

    def _extract_videos(self, message_div, avatar_urls: Set[str]) -> List[Dict[str, Any]]:
        """Extract video media files with enhanced detection."""
        videos = []

        for selector in self.media_selectors['video']:
            elements = message_div.select(selector)
            for element in elements:
                video_data = self._extract_single_video(element, avatar_urls)
                if video_data:
                    videos.append(video_data)

        return videos

    def _extract_single_video(self, element, avatar_urls: Set[str]) -> Optional[Dict[str, Any]]:
        """Extract single video from element with comprehensive URL detection."""
        if element.name == 'video':
            # Direct video element
            src = element.get('src')
            poster = element.get('poster')
            if src:
                return {
                    'url': self._normalize_video_url(src),
                    'type': 'video',
                    'thumbnail': self._normalize_image_url(poster) if poster else None,
                    'duration': self._extract_duration(element),
                    'source': 'direct_video'
                }

        # Video container - comprehensive URL extraction
        video_url = self._find_video_url_in_container(element)
        thumb_url = self._find_thumbnail_in_container(element, avatar_urls)

        if video_url or thumb_url:
            return {
                'url': video_url or thumb_url,  # Prefer actual video URL
                'type': 'video',
                'thumbnail': thumb_url,
                'duration': self._extract_duration(element),
                'source': 'container_video' if video_url else 'thumbnail_fallback'
            }

        return None

    def _find_video_url_in_container(self, element) -> Optional[str]:
        """Find actual video URL in container element."""
        # Check for video URLs in various attributes and data fields
        video_url_attrs = [
            'data-video', 'data-src', 'data-video-src', 'data-url',
            'href', 'data-href', 'data-video-url', 'data-mp4'
        ]

        for attr in video_url_attrs:
            url = element.get(attr)
            if url and self._is_video_url(url):
                return self._normalize_video_url(url)

        # Look for video URLs in nested elements
        for nested in element.select('a, source, [data-video]'):
            for attr in video_url_attrs:
                url = nested.get(attr)
                if url and self._is_video_url(url):
                    return self._normalize_video_url(url)

        return None

    def _find_thumbnail_in_container(self, element, avatar_urls: Set[str]) -> Optional[str]:
        """Find thumbnail image in container element."""
        thumb_img = element.select_one('img')
        if thumb_img:
            thumb_src = thumb_img.get('src') or thumb_img.get('data-src')
            if thumb_src and thumb_src not in avatar_urls:
                normalized_thumb = self._normalize_image_url(thumb_src)
                if normalized_thumb:
                    return normalized_thumb
        return None

    def _is_video_url(self, url: str) -> bool:
        """Check if URL points to a video file."""
        if not url:
            return False
        url_lower = url.lower()
        video_extensions = ['.mp4', '.avi', '.mov', '.wmv', '.flv', '.webm', '.mkv', '.m4v']
        return any(ext in url_lower for ext in video_extensions) or 'video' in url_lower

    def _normalize_video_url(self, url: str) -> Optional[str]:
        """Normalize video URL for Telegram CDN."""
        if not url:
            return None

        # Handle relative URLs
        if url.startswith('//'):
            url = f'https:{url}'
        elif url.startswith('/'):
            url = f'https://t.me{url}'

        # Handle Telegram CDN video URLs
        if 'cdn' in url and 't.me' in url:
            import re
            match = re.search(r'cdn\d*\.t\.me/file/(.*)', url)
            if match:
                return f'https://t.me/file/{match.group(1)}'

        return url if url.startswith(('http://', 'https://')) else None

    def _extract_duration(self, element) -> Optional[str]:
        """Extract video/audio duration from element."""
        duration_attrs = ['duration', 'data-duration', 'data-time']
        for attr in duration_attrs:
            duration = element.get(attr)
            if duration:
                return duration

        # Look for duration in text content
        import re
        text = element.get_text(strip=True)
        duration_match = re.search(r'(\d{1,2}:\d{2})', text)
        if duration_match:
            return duration_match.group(1)

        return None

    def _extract_documents(self, message_div, avatar_urls: Set[str]) -> List[Dict[str, Any]]:
        """Extract document media files."""
        documents = []
        for selector in self.media_selectors['document']:
            elements = message_div.select(selector)
            for element in elements:
                thumb_img = element.select_one('img')
                if thumb_img:
                    thumb_src = thumb_img.get('src') or thumb_img.get('data-src')
                    if thumb_src and thumb_src not in avatar_urls:
                        normalized_thumb = self._normalize_image_url(thumb_src)
                        if normalized_thumb:
                            documents.append({
                                'url': normalized_thumb,  # Use thumbnail as URL for now
                                'type': 'document',
                                'thumbnail': normalized_thumb
                            })
        return documents

    def _extract_audio(self, message_div, avatar_urls: Set[str]) -> List[Dict[str, Any]]:
        """Extract audio media files (voice messages, audio files)."""
        audio_files = []

        for selector in self.media_selectors['audio']:
            elements = message_div.select(selector)
            for element in elements:
                audio_data = self._extract_single_audio(element, avatar_urls)
                if audio_data:
                    audio_files.append(audio_data)

        return audio_files

    def _extract_single_audio(self, element, avatar_urls: Set[str]) -> Optional[Dict[str, Any]]:
        """Extract single audio file with comprehensive detection."""
        if element.name == 'audio':
            # Direct audio element
            src = element.get('src')
            if src:
                return {
                    'url': self._normalize_video_url(src),  # Reuse video URL normalization
                    'type': 'audio',
                    'duration': self._extract_duration(element),
                    'source': 'direct_audio'
                }

        # Audio container - look for audio URLs
        audio_url = self._find_audio_url_in_container(element)
        if audio_url:
            return {
                'url': audio_url,
                'type': 'audio',
                'duration': self._extract_duration(element),
                'source': 'container_audio'
            }

        return None

    def _find_audio_url_in_container(self, element) -> Optional[str]:
        """Find audio URL in container element."""
        audio_url_attrs = [
            'data-audio', 'data-src', 'data-audio-src', 'data-url',
            'href', 'data-href', 'data-audio-url', 'data-ogg', 'data-mp3'
        ]

        for attr in audio_url_attrs:
            url = element.get(attr)
            if url and self._is_audio_url(url):
                return self._normalize_video_url(url)

        return None

    def _is_audio_url(self, url: str) -> bool:
        """Check if URL points to an audio file."""
        if not url:
            return False
        url_lower = url.lower()
        audio_extensions = ['.mp3', '.wav', '.ogg', '.m4a', '.aac', '.flac', '.opus']
        return any(ext in url_lower for ext in audio_extensions) or 'audio' in url_lower or 'voice' in url_lower

    def _extract_stickers(self, message_div, avatar_urls: Set[str]) -> List[Dict[str, Any]]:
        """Extract stickers and animated stickers."""
        stickers = []

        for selector in self.media_selectors['sticker']:
            elements = message_div.select(selector)
            for element in elements:
                sticker_data = self._extract_single_sticker(element, avatar_urls)
                if sticker_data:
                    stickers.append(sticker_data)

        return stickers

    def _extract_single_sticker(self, element, avatar_urls: Set[str]) -> Optional[Dict[str, Any]]:
        """Extract single sticker."""
        # Look for sticker image
        sticker_img = element.select_one('img')
        if sticker_img:
            sticker_src = sticker_img.get('src') or sticker_img.get('data-src')
            if sticker_src and sticker_src not in avatar_urls:
                normalized_url = self._normalize_image_url(sticker_src)
                if normalized_url:
                    is_animated = 'animated' in element.get('class', []) or '.webp' in sticker_src or '.tgs' in sticker_src
                    return {
                        'url': normalized_url,
                        'type': 'animated_sticker' if is_animated else 'sticker',
                        'thumbnail': normalized_url,
                        'source': 'sticker'
                    }

        return None

    def _extract_gifs(self, message_div, avatar_urls: Set[str]) -> List[Dict[str, Any]]:
        """Extract GIF animations."""
        gifs = []

        for selector in self.media_selectors['gif']:
            elements = message_div.select(selector)
            for element in elements:
                gif_data = self._extract_single_gif(element, avatar_urls)
                if gif_data:
                    gifs.append(gif_data)

        return gifs

    def _extract_single_gif(self, element, avatar_urls: Set[str]) -> Optional[Dict[str, Any]]:
        """Extract single GIF."""
        # Look for GIF URL
        gif_url = self._find_gif_url_in_container(element)
        thumb_url = self._find_thumbnail_in_container(element, avatar_urls)

        if gif_url or thumb_url:
            return {
                'url': gif_url or thumb_url,
                'type': 'gif',
                'thumbnail': thumb_url,
                'source': 'gif'
            }

        return None

    def _find_gif_url_in_container(self, element) -> Optional[str]:
        """Find GIF URL in container element."""
        gif_url_attrs = [
            'data-gif', 'data-src', 'data-gif-src', 'data-url',
            'href', 'data-href', 'data-gif-url'
        ]

        for attr in gif_url_attrs:
            url = element.get(attr)
            if url and self._is_gif_url(url):
                return self._normalize_video_url(url)

        return None

    def _is_gif_url(self, url: str) -> bool:
        """Check if URL points to a GIF file."""
        if not url:
            return False
        url_lower = url.lower()
        return '.gif' in url_lower or 'gif' in url_lower

    def _extract_polls(self, message_div) -> List[Dict[str, Any]]:
        """Extract poll data."""
        polls = []

        for selector in self.media_selectors['poll']:
            elements = message_div.select(selector)
            for element in elements:
                poll_data = self._extract_single_poll(element)
                if poll_data:
                    polls.append(poll_data)

        return polls

    def _extract_single_poll(self, element) -> Optional[Dict[str, Any]]:
        """Extract single poll."""
        # Extract poll question and options
        question_elem = element.select_one('.poll_question, .poll_title')
        question = question_elem.get_text(strip=True) if question_elem else "Poll"

        options = []
        for option_elem in element.select('.poll_option, .poll_answer'):
            option_text = option_elem.get_text(strip=True)
            if option_text:
                options.append(option_text)

        if question or options:
            return {
                'type': 'poll',
                'url': f"#poll_{hash(question + ''.join(options))}",  # Generate unique identifier
                'poll_data': {
                    'question': question,
                    'options': options
                },
                'source': 'poll'
            }

        return None

    def _extract_locations(self, message_div) -> List[Dict[str, Any]]:
        """Extract location shares."""
        locations = []

        for selector in self.media_selectors['location']:
            elements = message_div.select(selector)
            for element in elements:
                location_data = self._extract_single_location(element)
                if location_data:
                    locations.append(location_data)

        return locations

    def _extract_single_location(self, element) -> Optional[Dict[str, Any]]:
        """Extract single location."""
        # Look for location data
        location_text = element.get_text(strip=True)
        location_url = element.get('href') or element.select_one('a').get('href') if element.select_one('a') else None

        if location_text or location_url:
            return {
                'type': 'location',
                'url': location_url or f"#location_{hash(location_text)}",
                'location_data': {
                    'text': location_text,
                    'url': location_url
                },
                'source': 'location'
            }

        return None

    def _extract_contacts(self, message_div) -> List[Dict[str, Any]]:
        """Extract contact shares."""
        contacts = []

        for selector in self.media_selectors['contact']:
            elements = message_div.select(selector)
            for element in elements:
                contact_data = self._extract_single_contact(element)
                if contact_data:
                    contacts.append(contact_data)

        return contacts

    def _extract_single_contact(self, element) -> Optional[Dict[str, Any]]:
        """Extract single contact."""
        # Look for contact data
        contact_text = element.get_text(strip=True)
        phone_elem = element.select_one('.contact_phone, .phone')
        phone = phone_elem.get_text(strip=True) if phone_elem else None

        if contact_text:
            return {
                'type': 'contact',
                'url': f"#contact_{hash(contact_text)}",
                'contact_data': {
                    'text': contact_text,
                    'phone': phone
                },
                'source': 'contact'
            }

        return None

    # CSS classes on elements whose background-image is never content
    BG_IMAGE_EXCLUDED_CLASSES = {
        'tg-emoji', 'emoji', 'tg-emoji-wrap',          # custom & regular emoji stickers
        'tgme_widget_message_user_photo',               # channel/user avatar
        'tgme_widget_message_owner_photo',              # owner avatar
        'message_author_photo', 'avatar', 'channel_photo',  # other avatars
        'bgcolor0', 'bgcolor1', 'bgcolor2', 'bgcolor3',
        'bgcolor4', 'bgcolor5', 'bgcolor6', 'bgcolor7',  # color placeholders for avatars
    }

    def _extract_background_images(self, message_div, avatar_urls: Set[str]) -> List[Dict[str, Any]]:
        """Extract background images from style attributes, skipping emoji and avatar elements."""
        bg_images = []
        for element in message_div.select('[style*="background-image"]'):
            # Skip elements that are emoji or avatar by CSS class
            el_classes = set(element.get('class', []))
            if el_classes & self.BG_IMAGE_EXCLUDED_CLASSES:
                continue

            style = element.get('style', '')
            bg_matches = re.findall(r'background-image:\s*url\(["\']?(.*?)["\']?\)', style)
            for match in bg_matches:
                if match not in avatar_urls:
                    normalized_url = self._normalize_image_url(match)
                    if normalized_url and self._is_content_image(normalized_url):
                        bg_images.append({
                            'url': normalized_url,
                            'type': 'image',
                            'thumbnail': normalized_url
                        })
        return bg_images

    def _deduplicate_media(self, media_files: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Remove duplicate media files based on URL."""
        seen_urls = set()
        unique_media = []
        for media in media_files:
            if media['url'] not in seen_urls:
                seen_urls.add(media['url'])
                unique_media.append(media)
        return unique_media

    def _extract_video_info(self, message_div, media_info: Dict[str, Any]) -> None:
        """Extract video information."""
        video_selectors = [
            'video[src]',
            '.video-message video',
            '.media-video video',
            'source[src]'
        ]

        for selector in video_selectors:
            video = message_div.select_one(selector)
            if video and video.get('src'):
                media_info['video_url'] = self._normalize_image_url(video['src'])
                media_info['media_type'] = 'video'
                if video.get('duration'):
                    media_info['duration'] = video['duration']
                break

    def _extract_document_info(self, message_div, media_info: Dict[str, Any]) -> None:
        """Extract document/file information."""
        doc_selectors = [
            'a[href*="/file/"]',
            '.document a[href]',
            '.attachment a[href]',
            '.file-download a[href]'
        ]

        for selector in doc_selectors:
            doc_link = message_div.select_one(selector)
            if doc_link and doc_link.get('href'):
                media_info['document_url'] = self._normalize_image_url(doc_link['href'])
                media_info['media_type'] = 'document'

                # Try to extract file name
                file_name_element = doc_link.select_one('.file-name, .document-name')
                if file_name_element:
                    media_info['file_name'] = file_name_element.get_text(strip=True)

                # Try to extract file size
                file_size_element = message_div.select_one('.file-size, .document-size')
                if file_size_element:
                    media_info['file_size'] = file_size_element.get_text(strip=True)
                break

    def _extract_audio_info(self, message_div, media_info: Dict[str, Any]) -> None:
        """Extract audio information."""
        audio_selectors = [
            'audio[src]',
            '.audio-message audio',
            '.voice-message audio'
        ]

        for selector in audio_selectors:
            audio = message_div.select_one(selector)
            if audio and audio.get('src'):
                media_info['audio_url'] = self._normalize_image_url(audio['src'])
                media_info['media_type'] = 'audio'
                if audio.get('duration'):
                    media_info['duration'] = audio['duration']
                break

    def _extract_sticker_info(self, message_div, media_info: Dict[str, Any]) -> None:
        """Extract sticker information."""
        sticker = message_div.select_one('.sticker img, .animated-sticker img')
        if sticker and sticker.get('src'):
            media_info['sticker_url'] = self._normalize_image_url(sticker['src'])
            media_info['media_type'] = 'sticker'

    def _extract_poll_info(self, message_div, media_info: Dict[str, Any]) -> None:
        """Extract poll information."""
        poll = message_div.select_one('.poll, .quiz')
        if poll:
            poll_question = poll.select_one('.poll-question, .quiz-question')
            poll_options = poll.select('.poll-option, .quiz-option')

            if poll_question:
                media_info['poll_data'] = {
                    'question': poll_question.get_text(strip=True),
                    'options': [opt.get_text(strip=True) for opt in poll_options],
                    'type': 'quiz' if 'quiz' in poll.get('class', []) else 'poll'
                }
                media_info['media_type'] = 'poll'

    def _extract_location_info(self, message_div, media_info: Dict[str, Any]) -> None:
        """Extract location information."""
        location = message_div.select_one('.location, .venue')
        if location:
            location_text = location.get_text(strip=True)
            # Try to extract coordinates from data attributes or href
            location_link = location.select_one('a[href*="maps.google.com"], a[href*="openstreetmap.org"]')

            media_info['location_data'] = {
                'text': location_text,
                'url': location_link['href'] if location_link else None
            }
            media_info['media_type'] = 'location'

    def _normalize_image_url(self, url: str) -> Optional[str]:
        """Normalize image URL (implementation from original TelegramSource)."""
        if not url:
            return None

        # Remove CDN path prefixes
        if url.startswith('//'):
            url = f'https:{url}'
        elif url.startswith('/'):
            url = f'https://t.me{url}'

        # Handle Telegram CDN URLs
        if 'cdn' in url and 't.me' in url:
            # Extract the actual file path
            import re
            match = re.search(r'cdn\d*\.t\.me/file/(.*)', url)
            if match:
                return f'https://t.me/file/{match.group(1)}'

        return url if url.startswith(('http://', 'https://')) else None

    def _is_content_image(self, url: str) -> bool:
        """Check if URL points to a content image (implementation from original TelegramSource)."""
        if not url:
            return False

        # Skip emoji images
        if '/img/emoji/' in url.lower():
            return False

        # Skip channel logos and profile photos
        if any(pattern in url.lower() for pattern in ['/userpic/', '/channel_photo/', '/profile_photo/']):
            return False


        # Skip small images that are likely icons or avatars
        size_indicators = ['16x16', '32x32', '50x50', '64x64', 'thumb', 'icon', 'avatar']
        url_lower = url.lower()

        if any(indicator in url_lower for indicator in size_indicators):
            return False

        # Check file extension and Telegram CDN (but not if already filtered out above)
        content_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp', '.svg']
        if any(ext in url_lower for ext in content_extensions):
            # Only allow Telegram CDN images if they're not in excluded paths
            if 'telegram' in url_lower:
                # Already passed emoji/profile/channel checks above, so it's content
                return True
            else:
                # Non-Telegram image
                return True

        # If no clear extension but looks like Telegram CDN (and passed filters)
        if 't.me' in url_lower or 'cdn.telegram' in url_lower:
            return True

        return False
