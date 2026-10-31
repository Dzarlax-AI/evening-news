interface ReaderMedia {
  type?: string;
  url?: string | null;
  filename?: string;
  thumbnail_url?: string;
  thumbnail?: string;
}
interface ReaderCategory {
  name?: string;
  display_name?: string;
  ai_category?: string;
  category?: string;
  confidence?: number;
  color?: string;
  mapped_to?: string;
}
interface ReaderArticle {
  title?: string;
  summary?: string;
  content?: string;
  source_name?: string;
  published_at?: string;
  url?: string;
  category?: string;
  categories?: ReaderCategory[];
  ai_categories?: ReaderCategory[];
  is_advertisement?: boolean;
  ad_reasoning?: string;
  ad_markers?: string[];
  image_url?: string;
  primary_image?: string;
  media_files?: ReaderMedia[];
}
interface Window {
  articleReader: {
    render(article: ReaderArticle): void;
    reset(): void;
    safeURL(value: unknown): string;
    navigate(direction: number): boolean;
    fullscreen(): void;
  };
  navigateGallery(direction: number): void;
  toggleFullscreen(): void;
  toggleContentSpoiler(): void;
}
(() => {
  const el = (id: string): HTMLElement => document.getElementById(id)!;
  const root = (): HTMLElement => document.querySelector('[data-article-reader]')!;
  let fallback = '';
  let fallbackAttempted = false;
  let failedImages = new Set<string>();

  const safeURL = (value: unknown): string => {
    if (typeof value !== 'string') return '';
    try {
      const url = new URL(value);
      return (url.protocol === 'http:' || url.protocol === 'https:') && !url.username && !url.password ? url.href : '';
    } catch {
      return '';
    }
  };
  const playable = (type: string, url: string): boolean => {
    const path = new URL(url).pathname.toLowerCase();
    return (type === 'video' && /\.(mp4|webm|ogg)$/.test(path)) ||
      (type === 'audio' && /\.(mp3|wav|m4a|ogg|opus)$/.test(path));
  };
  const select = (index: number): void => {
    el('galleryImages').querySelectorAll('img').forEach((image, i) => {
      image.classList.toggle('active', i === index);
      if (i === index) image.loading = 'eager';
    });
    el('galleryThumbnails').querySelectorAll('button').forEach((button, i) => button.classList.toggle('active', i === index));
  };
  const addImage = (url: string, label: string, thumbnailURL = url): void => {
    const images = el('galleryImages');
    const thumbs = el('galleryThumbnails');
    const index = images.children.length;
    const image = document.createElement('img');
    image.src = url;
    image.alt = label;
    image.loading = index === 0 ? 'eager' : 'lazy';
    image.className = 'gallery-image' + (index === 0 ? ' active' : '');
    images.append(image);
    const button = document.createElement('button');
    button.type = 'button';
    button.dataset.imageIndex = String(index);
    button.className = 'gallery-thumbnail' + (index === 0 ? ' active' : '');
    button.setAttribute('aria-label', label);
    const thumbnail = document.createElement('img');
    thumbnail.src = thumbnailURL;
    thumbnail.loading = 'lazy';
    thumbnail.alt = '';
    button.append(thumbnail);
    thumbs.append(button);
    el('modalGallery').style.display = '';
  };
  const imageFailed = (image: HTMLImageElement): void => {
    const images = Array.from(el('galleryImages').querySelectorAll('img'));
    const index = images.indexOf(image);
    if (index < 0) {
      const button = image.closest<HTMLButtonElement>('button[data-image-index]');
      const original = button ? images[Number(button.dataset.imageIndex)] : null;
      if (original && image.src !== original.src) image.src = original.src;
      else if (button) button.hidden = true;
      return;
    }
    failedImages.add(image.src);
    const active = image.classList.contains('active');
    image.remove();
    el('galleryThumbnails').querySelectorAll('button')[index]?.remove();
    el('galleryThumbnails').querySelectorAll('button').forEach((button, i) => button.dataset.imageIndex = String(i));
    if (el('galleryImages').querySelector('img')) {
      if (active) select(0);
    } else if (!fallbackAttempted && fallback && !failedImages.has(fallback)) {
      fallbackAttempted = true;
      addImage(fallback, 'Image');
    } else {
      el('modalGallery').style.display = 'none';
      el('articleImageError').hidden = false;
    }
  };
  const navigate = (direction: number): boolean => {
    const images = Array.from(el('galleryImages').querySelectorAll('img'));
    if (images.length < 2) return false;
    const current = images.findIndex(image => image.classList.contains('active'));
    select((current + direction + images.length) % images.length);
    return true;
  };
  const fullscreen = (): void => {
    const gallery = el('modalGallery');
    if (document.fullscreenElement) void document.exitFullscreen().catch(() => {});
    else if (gallery.requestFullscreen && gallery.style.display !== 'none') void gallery.requestFullscreen().catch(() => {});
  };
  const init = (): void => {
    const gallery = el('modalGallery');
    if (!gallery || gallery.dataset.ready) return;
    gallery.dataset.ready = 'true';
    document.addEventListener('keydown', event => {
      const modal = document.getElementById('articleModal');
      if (modal && modal.style.display === 'none') return;
      if (event.altKey || event.ctrlKey || event.metaKey || event.shiftKey) return;
      const target = event.target as HTMLElement;
      if (target.isContentEditable || ['INPUT', 'TEXTAREA', 'SELECT', 'AUDIO', 'VIDEO'].includes(target.tagName)) return;
      if (!modal && !gallery.contains(target)) return;
      if ((event.key === 'ArrowLeft' || event.key === 'ArrowRight') && navigate(event.key === 'ArrowLeft' ? -1 : 1)) event.preventDefault();
    });
    fallback = safeURL(root().dataset.fallbackImage);
    el('galleryThumbnails').addEventListener('click', event => {
      const button = (event.target as Element).closest<HTMLButtonElement>('button[data-image-index]');
      if (button) select(Number(button.dataset.imageIndex));
    });
    gallery.querySelector('.gallery-fullscreen')?.addEventListener('click', fullscreen);
    gallery.addEventListener('error', event => {
      const image = event.target as HTMLImageElement;
      if (image.tagName === 'IMG') imageFailed(image);
    }, true);
    el('articleMediaLinks').addEventListener('error', event => {
      const media = event.target as HTMLMediaElement;
      if (media.tagName === 'VIDEO' || media.tagName === 'AUDIO') {
        const link = media.querySelector('a');
        if (link) media.replaceWith(link);
      }
    }, true);
    gallery.querySelectorAll<HTMLImageElement>('#galleryImages img').forEach(image => {
      if (image.complete && image.naturalWidth === 0) imageFailed(image);
    });
  };
  const reset = (): void => {
    if (document.fullscreenElement === el('modalGallery')) void document.exitFullscreen().catch(() => {});
    (el('modalContentSpoiler') as HTMLDetailsElement).open = false;
    el('modalGallery').style.display = 'none';
    el('articleImageError').hidden = true;
    el('modalCategory').style.removeProperty('background');
    el('modalCategory').style.removeProperty('color');
    el('modalDate').removeAttribute('datetime');
    ['galleryImages', 'galleryThumbnails', 'articleMediaLinks', 'readerSourceLink', 'modalCategory', 'modalContent', 'modalFinalCategories', 'originalCategoriesList', 'modalAdReasoningText', 'modalAdMarkersList'].forEach(id => el(id).replaceChildren());
    fallback = '';
    fallbackAttempted = false;
    failedImages = new Set<string>();
  };
  const categoryColors = (value: unknown): [string, string] => {
    const color = typeof value === 'string' && /^#[0-9a-fA-F]{6}$/.test(value) ? value : '#6c757d';
    const rgb = parseInt(color.slice(1), 16);
    const luminance = 299 * ((rgb >> 16) & 255) + 587 * ((rgb >> 8) & 255) + 114 * (rgb & 255);
    return [color, luminance > 127500 ? '#000000' : '#ffffff'];
  };
  const renderCategories = (id: string, categories: ReaderCategory[], ai: boolean): void => {
    const container = el(id);
    container.replaceChildren();
    categories.forEach(category => {
      if (!category || typeof category !== 'object' || Array.isArray(category)) return;
      const fields = ai ? [category.ai_category, category.name, category.category] : [category.display_name, category.name, category.category];
      const name = fields.find(value => typeof value === 'string' && value);
      if (!name) return;
      const span = document.createElement('span');
      span.className = ai ? 'category-item' : 'modal-final-category';
      if (!ai) { const [background, text] = categoryColors(category.color); span.style.background = background; span.style.color = text; span.style.borderColor = background; }
      span.textContent = `${name}${ai && category.mapped_to ? ' → ' + category.mapped_to : ''} · ${Math.round((category.confidence || 0) * 100)}%`;
      container.append(span);
    });
  };
  const render = (article: ReaderArticle): void => {
    init();
    reset();
    const categories = Array.isArray(article.categories) ? article.categories : [];
    const aiCategories = Array.isArray(article.ai_categories) ? article.ai_categories : [];
    const firstCategory = categories.find(category => category && typeof category === 'object' && (typeof category.display_name === 'string' && category.display_name || typeof category.name === 'string' && category.name));
    const categoryName = typeof firstCategory?.display_name === 'string' && firstCategory.display_name ? firstCategory.display_name : firstCategory?.name;
    const category = el('modalCategory');
    category.textContent = categoryName || article.category || '';
    const [background, text] = categoryColors(firstCategory?.color);
    category.style.background = background;
    category.style.color = text;
    renderCategories('modalFinalCategories', categories, false);
    el('modalFinalCategories').style.display = categories.length ? 'flex' : 'none';
    renderCategories('originalCategoriesList', aiCategories, true);
    el('modalOriginalCategories').style.display = aiCategories.length ? '' : 'none';
    el('modalAdBadge').style.display = article.is_advertisement ? '' : 'none';
    el('modalAdReasoningText').textContent = article.ad_reasoning || '';
    el('modalAdReasoning').style.display = article.is_advertisement && article.ad_reasoning ? '' : 'none';
    const markers = Array.isArray(article.ad_markers) ? article.ad_markers.filter(marker => typeof marker === 'string') : [];
    for (const marker of markers) { const item = document.createElement('li'); item.textContent = marker; el('modalAdMarkersList').append(item); }
    el('modalAdMarkers').style.display = article.is_advertisement && article.ad_reasoning && markers.length ? '' : 'none';
    el('modalTitle').textContent = article.title || '';
    el('modalSource').textContent = article.source_name || '';
    const rawDate = article.published_at || '';
    const date = rawDate ? new Date(/(?:Z|[+-]\d\d:\d\d)$/i.test(rawDate) ? rawDate : rawDate + 'Z') : null;
    const pad = (n: number): string => String(n).padStart(2, '0');
    const dateElement = el('modalDate') as HTMLTimeElement;
    dateElement.dateTime = date && !isNaN(date.valueOf()) ? date.toISOString().replace('.000Z', 'Z') : rawDate;
    dateElement.textContent = date && !isNaN(date.valueOf()) ? `${pad(date.getUTCDate())}.${pad(date.getUTCMonth() + 1)}.${date.getUTCFullYear()}, ${pad(date.getUTCHours())}:${pad(date.getUTCMinutes())} UTC` : '';
    const summary = el('modalSummary');
    summary.replaceChildren();
    summary.style.display = article.summary ? '' : 'none';
    if (article.summary) {
      const paragraph = document.createElement('p');
      paragraph.textContent = article.summary;
      summary.append(paragraph);
    }
    const content = el('modalContent');
    (article.content || '').split('\n').map(line => line.trim()).filter(Boolean).forEach(text => {
      const paragraph = document.createElement('p');
      paragraph.textContent = text;
      content.append(paragraph);
    });
    el('modalContentSpoiler').style.display = article.content ? '' : 'none';
    const sourceURL = safeURL(article.url);
    if (sourceURL) {
      const link = document.createElement('a');
      link.href = sourceURL;
      link.target = '_blank';
      link.rel = 'noopener noreferrer';
      link.textContent = document.documentElement.lang === 'sr-Latn' ? 'Otvori original' : 'Открыть оригинал';
      el('readerSourceLink').append(link);
    }
    fallback = safeURL(article.image_url) || safeURL(article.primary_image);
    const seen = new Set<string>();
    for (const media of Array.isArray(article.media_files) ? article.media_files : []) {
      if (!media || typeof media !== 'object' || Array.isArray(media)) continue;
      const url = safeURL(media.url);
      if (!url || seen.has(url)) continue;
      if (media.type === 'image') {
        const thumbnail = safeURL(media.thumbnail_url) || safeURL(media.thumbnail) || url;
        addImage(url, media.filename || 'Image', thumbnail);
      } else if (['video', 'audio', 'document', 'gif', 'sticker', 'animated_sticker'].includes(media.type || '')) {
        const link = document.createElement('a');
        link.href = url;
        link.target = '_blank';
        link.rel = 'noopener noreferrer';
        link.textContent = media.filename || media.type || '';
        if (playable(media.type || '', url)) {
          const player = document.createElement(media.type as 'audio' | 'video');
          player.controls = true;
          player.preload = 'none';
          player.src = url;
          player.append(link);
          el('articleMediaLinks').append(player);
        } else {
          el('articleMediaLinks').append(link);
        }
      } else continue;
      seen.add(url);
    }
    if (!el('galleryImages').children.length && fallback) {
      fallbackAttempted = true;
      addImage(fallback, 'Image');
    }
  };
  window.articleReader = {render, reset, safeURL, navigate, fullscreen};
  window.navigateGallery = direction => { navigate(direction); };
  window.toggleFullscreen = fullscreen;
  window.toggleContentSpoiler = () => {
    const details = el('modalContentSpoiler') as HTMLDetailsElement;
    details.open = !details.open;
  };
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
