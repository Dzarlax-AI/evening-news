"""Universal message splitter for Telegram and other messaging platforms."""

import logging
from typing import List, Dict, Any, Tuple
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class MessagePart:
    """Represents a single part of a split message."""
    content: str
    part_number: int
    total_parts: int


@dataclass
class SplitResult:
    """Result of message splitting operation."""
    parts: List[MessagePart]
    original_length: int
    total_parts: int
    was_split: bool


class MessageSplitter:
    """Universal message splitter with configurable limits."""

    def __init__(self, max_length: int = 3600, safety_margin: int = 100):
        """
        Initialize message splitter.

        Args:
            max_length: Maximum length for a single message
            safety_margin: Safety margin to avoid hitting limits exactly
        """
        self.max_length = max_length
        self.safety_margin = safety_margin
        self.effective_limit = max_length - safety_margin

    def split_digest(
        self,
        header: str,
        content_blocks: List[Tuple[str, str]],
        footer: str,
        metadata: Dict[str, Any] = None
    ) -> SplitResult:
        """
        Split digest message into multiple parts if needed.

        Args:
            header: Message header (included in each part)
            content_blocks: List of (title, content) tuples to include
            footer: Message footer (included in last part only)
            metadata: Optional metadata like total_articles, categories_count

        Returns:
            SplitResult containing all message parts
        """
        original_length = (
            len(header) +
            sum(len(title) + len(content) for title, content in content_blocks) +
            len(footer)
        )

        # Try to fit in single message first
        if original_length <= self.effective_limit:
            single_message = self._build_single_message(
                header, content_blocks, footer
            )
            return SplitResult(
                parts=[MessagePart(single_message, 1, 1)],
                original_length=original_length,
                total_parts=1,
                was_split=False
            )

        # Need to split
        logger.info(
            f"Message too long ({original_length} chars), "
            f"splitting into multiple parts"
        )

        parts = self._split_message_parts(
            header, content_blocks, footer, metadata
        )

        return SplitResult(
            parts=parts,
            original_length=original_length,
            total_parts=len(parts),
            was_split=True
        )

    def _build_single_message(
        self,
        header: str,
        content_blocks: List[Tuple[str, str]],
        footer: str
    ) -> str:
        """Build single message from components."""
        parts = [header, ""]

        for title, content in content_blocks:
            if title and content:
                parts.append(f"<b>{title}</b>\n{content.strip()}\n")

        parts.append(footer)

        return "\n".join(parts)

    def _split_message_parts(
        self,
        header: str,
        content_blocks: List[Tuple[str, str]],
        footer: str,
        metadata: Dict[str, Any] = None
    ) -> List[MessagePart]:
        """Split message into multiple parts."""
        parts = []
        current_blocks = []
        current_length = len(header) + 2  # header + empty line

        for i, (title, content) in enumerate(content_blocks):
            if not title or not content:
                continue

            block_length = len(title) + len(content) + 4  # title + content + formatting
            estimated_part_footer = self._estimate_part_footer(
                len(parts) + 1,
                len(current_blocks) + 1,
                metadata
            )

            # Check if block fits in current part
            if current_length + block_length + estimated_part_footer <= self.effective_limit:
                # Fits in current part
                current_blocks.append((title, content))
                current_length += block_length
            else:
                # Start new part
                if current_blocks:
                    # Save current part
                    part_content = self._build_part_content(
                        header,
                        current_blocks,
                        len(parts) + 1,
                        0,  # Unknown total yet
                        metadata,
                        is_final=False
                    )
                    parts.append(MessagePart(
                        part_content,
                        len(parts) + 1,
                        0  # Will update later
                    ))

                # Start new part with current block
                current_blocks = [(title, content)]
                current_length = len(header) + 2 + block_length

        # Add final part
        if current_blocks:
            part_content = self._build_part_content(
                header,
                current_blocks,
                len(parts) + 1,
                len(parts) + 1,
                metadata,
                is_final=True,
                footer=footer
            )
            parts.append(MessagePart(
                part_content,
                len(parts) + 1,
                len(parts) + 1
            ))

        # Update total_parts in all parts
        total_parts = len(parts)
        for part in parts:
            part.total_parts = total_parts

        logger.info(f"Split message into {total_parts} parts")
        return parts

    def _build_part_content(
        self,
        header: str,
        blocks: List[Tuple[str, str]],
        part_number: int,
        total_parts: int,
        metadata: Dict[str, Any] = None,
        is_final: bool = False,
        footer: str = ""
    ) -> str:
        """Build content for a single message part."""
        content_parts = [header, ""]

        # Add content blocks
        for title, content in blocks:
            content_parts.append(f"<b>{title}</b>\n{content.strip()}\n")

        # Add footer
        if is_final and footer:
            content_parts.append(footer)
        elif metadata:
            # Add intermediate footer
            part_footer = self._build_intermediate_footer(
                part_number,
                len(blocks),
                metadata
            )
            content_parts.append(part_footer)
            content_parts.append("\n💬 Продолжение следует...")

        return "\n".join(content_parts)

    def _estimate_part_footer(
        self,
        part_number: int,
        blocks_count: int,
        metadata: Dict[str, Any] = None
    ) -> int:
        """Estimate length of intermediate footer."""
        if not metadata:
            return 50  # Default estimate

        footer = self._build_intermediate_footer(
            part_number, blocks_count, metadata
        )
        return len(footer) + 30  # footer + continuation message

    def _build_intermediate_footer(
        self,
        part_number: int,
        blocks_count: int,
        metadata: Dict[str, Any] = None
    ) -> str:
        """Build intermediate part footer."""
        if metadata:
            total_articles = metadata.get("total_articles", 0)
            total_categories = metadata.get("categories_count", 0)
            return f"\n📊 Часть {part_number} • {blocks_count} из {total_categories} категорий"
        else:
            return f"\n📊 Часть {part_number}"

    def split_text_simple(
        self,
        text: str,
        max_length: int = None
    ) -> List[str]:
        """
        Simple text splitter without header/footer handling.

        Args:
            text: Text to split
            max_length: Override default max length

        Returns:
            List of text parts
        """
        limit = max_length or self.effective_limit

        if len(text) <= limit:
            return [text]

        parts = []
        current_part = ""
        lines = text.split("\n")

        for line in lines:
            if len(current_part) + len(line) + 1 <= limit:
                current_part += line + "\n"
            else:
                if current_part:
                    parts.append(current_part.rstrip())
                current_part = line + "\n"

        if current_part:
            parts.append(current_part.rstrip())

        return parts

    def validate_message_length(self, message: str) -> Tuple[bool, int]:
        """
        Validate if message fits within limits.

        Args:
            message: Message to validate

        Returns:
            Tuple of (is_valid, length)
        """
        length = len(message)
        is_valid = length <= self.max_length
        return is_valid, length


class TelegramMessageSplitter(MessageSplitter):
    """Telegram-specific message splitter with 4096 character limit."""

    def __init__(self, safety_margin: int = 200):
        """
        Initialize Telegram message splitter.

        Args:
            safety_margin: Safety margin for link or keyboard overhead
        """
        super().__init__(max_length=4096, safety_margin=safety_margin)
