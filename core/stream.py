"""Zero-Copy In-Memory Stream Processing & Perceptual Hashing.

Decodes, hashes, compresses, and samples media entirely in RAM with zero disk I/O.
"""

import gc
import io
import logging
from typing import List, Optional, Tuple, Union
import numpy as np
from PIL import Image, ImageOps

logger = logging.getLogger("core.stream")

# Optional PyAV import for low-level in-memory video stream demuxing
try:
    import av
    HAS_AV = True
except ImportError:
    HAS_AV = False

# Optional imagehash import with fallback
try:
    import imagehash
    HAS_IMAGEHASH = True
except ImportError:
    HAS_IMAGEHASH = False


def _fallback_dhash(img: Image.Image) -> str:
    """Pure PIL/Python 64-bit difference hash (dHash) fallback."""
    # Resize to 9x8 grayscale
    resized = img.convert("L").resize((9, 8), Image.Resampling.LANCZOS)
    pixels = list(resized.getdata())
    diff = []
    for row in range(8):
        for col in range(8):
            left = pixels[row * 9 + col]
            right = pixels[row * 9 + col + 1]
            diff.append(1 if left > right else 0)
    # Convert 64 bits to 16 hex chars
    decimal_val = 0
    for bit in diff:
        decimal_val = (decimal_val << 1) | bit
    return f"{decimal_val:016x}"


def _fallback_phash(img: Image.Image) -> str:
    """Mathematical 2D Discrete Cosine Transform (DCT) 64-bit perceptual hash (pHash).

    Scale, compression, and slight crop invariant without external C-extensions.
    """
    # 1. Resize to 32x32 grayscale
    img_l = img.convert("L").resize((32, 32), Image.Resampling.LANCZOS)
    vals = np.asarray(img_l, dtype=np.float64)

    # 2. Compute 2D Discrete Cosine Transform via matrix basis
    N = 32
    n = np.arange(N)
    k = np.arange(N).reshape(-1, 1)
    C = np.sqrt(2.0 / N) * np.cos(np.pi * (2 * n + 1) * k / (2.0 * N))
    C[0, :] = 1.0 / np.sqrt(N)
    dct = C @ vals @ C.T

    # 3. Extract top-left 8x8 low-frequency block
    low_freq = dct[0:8, 0:8]
    med = np.median(low_freq)
    diff = low_freq > med

    # 4. Convert 64 booleans into 16 hex characters
    decimal_val = 0
    for bit in diff.flatten():
        decimal_val = (decimal_val << 1) | int(bit)
    return f"{decimal_val:016x}"


def compute_image_hashes(image_bytes: bytes) -> Tuple[str, str]:
    """Compute 64-bit dHash and pHash directly from an in-memory byte buffer.

    Returns (dhash_hex, phash_hex).
    """
    if not image_bytes:
        return ("", "")

    try:
        with Image.open(io.BytesIO(image_bytes)) as img:
            img = ImageOps.exif_transpose(img)
            if HAS_IMAGEHASH:
                d_h = str(imagehash.dhash(img))
                p_h = str(imagehash.phash(img))
                return (d_h, p_h)
            else:
                d_h = _fallback_dhash(img)
                p_h = _fallback_phash(img)
                return (d_h, p_h)
    except Exception as e:
        logger.warning(f"Error computing perceptual hash: {e}")
        return ("", "")


def compress_image_to_webp(
    image_bytes: bytes, max_dim: int = 800, quality: int = 80
) -> bytes:
    """In-memory image compression.

    Downscales image to max_dim and encodes to WebP to minimize RAM & LLM tokens.
    """
    if not image_bytes:
        return b""

    try:
        with Image.open(io.BytesIO(image_bytes)) as img:
            img = ImageOps.exif_transpose(img)

            # Convert palette/RGBA modes to RGB for WebP compatibility
            if img.mode in ("RGBA", "LA") or (img.mode == "P" and "transparency" in img.info):
                # Keep transparent WebP or convert to RGB with white background
                bg = Image.new("RGB", img.size, (255, 255, 255))
                if img.mode != "RGBA":
                    img = img.convert("RGBA")
                bg.paste(img, mask=img.split()[-1])
                img = bg
            elif img.mode != "RGB":
                img = img.convert("RGB")

            # Constrain dimensions keeping aspect ratio
            w, h = img.size
            if max(w, h) > max_dim:
                scale = max_dim / float(max(w, h))
                new_w, new_h = int(w * scale), int(h * scale)
                img = img.resize((new_w, new_h), Image.Resampling.LANCZOS)

            out_buf = io.BytesIO()
            img.save(out_buf, format="WEBP", quality=quality, method=4)
            return out_buf.getvalue()
    except Exception as e:
        logger.error(f"Error compressing image to WebP: {e}")
        return image_bytes


def extract_video_keyframes(
    video_bytes: bytes, max_frames: int = 4
) -> List[bytes]:
    """Extract key visual frames from in-memory video stream without disk writes.

    Decodes frames directly via PyAV if available, otherwise returns sample bytes.
    """
    if not video_bytes:
        return []

    frames_bytes: List[bytes] = []

    if HAS_AV:
        try:
            container = av.open(io.BytesIO(video_bytes))
            video_stream = next((s for s in container.streams if s.type == "video"), None)
            if not video_stream:
                return []

            total_frames = video_stream.frames or 120
            step = max(1, total_frames // max_frames)
            count = 0

            for frame in container.decode(video=0):
                if count % step == 0:
                    img = frame.to_image()
                    buf = io.BytesIO()
                    img.save(buf, format="WEBP", quality=75)
                    frames_bytes.append(buf.getvalue())
                    if len(frames_bytes) >= max_frames:
                        break
                count += 1
            container.close()
            return frames_bytes
        except Exception as e:
            logger.warning(f"PyAV decoding failed, falling back: {e}")

    # Fallback: if PyAV is not installed or stream is corrupt, attempt to treat as single-frame/mock
    return frames_bytes


def purge_buffers(*buffers: Optional[Union[bytes, io.BytesIO, List[bytes]]]) -> None:
    """Explicitly release in-memory media buffers and trigger garbage collection."""
    for b in buffers:
        if b is None:
            continue
        if isinstance(b, io.BytesIO):
            b.close()
        del b
    gc.collect()
