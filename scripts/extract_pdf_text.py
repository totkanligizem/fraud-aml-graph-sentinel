#!/usr/bin/env python3
"""
Minimal PDF text extractor for this workspace.

Supported features:
- direct and object-stream (ObjStm) objects
- FlateDecode and ASCII85Decode streams
- Type0 fonts with ToUnicode CMaps
- text-show operators: Tj, TJ, ', "
"""

from __future__ import annotations

import argparse
import base64
import re
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union


Token = Union[str, Tuple[str, bytes], List["Token"]]


@dataclass
class FontInfo:
    name: str
    identity_h: bool
    cmap: Dict[int, str]


class PdfExtractor:
    def __init__(self, pdf_bytes: bytes) -> None:
        self.pdf_bytes = pdf_bytes
        self.objects: Dict[int, bytes] = self._load_objects(pdf_bytes)

    @staticmethod
    def _load_objects(pdf_bytes: bytes) -> Dict[int, bytes]:
        objects: Dict[int, bytes] = {}
        for match in re.finditer(rb"(\d+)\s+0\s+obj(.*?)endobj", pdf_bytes, re.S):
            obj_num = int(match.group(1))
            objects[obj_num] = match.group(2).strip()

        # Expand object streams (ObjStm). Iterate to handle multiple/recursive ObjStm cases.
        expanded = True
        while expanded:
            expanded = False
            for obj_num, body in list(objects.items()):
                if b"/Type /ObjStm" not in body or b"stream" not in body:
                    continue
                try:
                    stream = PdfExtractor._decode_stream(body)
                except Exception:
                    continue
                if stream is None:
                    continue
                n_match = re.search(rb"/N\s+(\d+)", body)
                first_match = re.search(rb"/First\s+(\d+)", body)
                if not n_match or not first_match:
                    continue
                n = int(n_match.group(1))
                first = int(first_match.group(1))
                if first <= 0 or first >= len(stream):
                    continue
                header = stream[:first].decode("latin1", "ignore")
                nums = [int(x) for x in header.split() if x.isdigit()]
                if len(nums) < n * 2:
                    continue
                pairs = [(nums[i], nums[i + 1]) for i in range(0, n * 2, 2)]
                for idx, (inner_obj_num, offset) in enumerate(pairs):
                    start = first + offset
                    end = first + (pairs[idx + 1][1] if idx + 1 < len(pairs) else len(stream) - first)
                    if start < 0 or end > len(stream) or start >= end:
                        continue
                    if inner_obj_num not in objects:
                        objects[inner_obj_num] = stream[start:end].strip()
                        expanded = True
        return objects

    @staticmethod
    def _extract_stream_raw(body: bytes) -> Optional[bytes]:
        stream_idx = body.find(b"stream")
        if stream_idx < 0:
            return None
        end_idx = body.find(b"endstream", stream_idx)
        if end_idx < 0:
            return None
        data = body[stream_idx + 6 : end_idx]
        if data.startswith(b"\r\n"):
            data = data[2:]
        elif data[:1] in (b"\r", b"\n"):
            data = data[1:]
        if data.endswith(b"\r\n"):
            data = data[:-2]
        elif data[-1:] in (b"\r", b"\n"):
            data = data[:-1]
        return data

    @staticmethod
    def _decode_stream(body: bytes) -> Optional[bytes]:
        data = PdfExtractor._extract_stream_raw(body)
        if data is None:
            return None
        filter_match = re.search(rb"/Filter\s*(\[[^\]]+\]|/\w+)", body)
        filters: List[bytes] = []
        if filter_match:
            val = filter_match.group(1)
            if val.startswith(b"["):
                filters = re.findall(rb"/([A-Za-z0-9]+)", val)
            else:
                filters = [val.lstrip(b"/")]

        out = data
        for flt in filters:
            if flt == b"FlateDecode":
                out = zlib.decompress(out)
            elif flt == b"ASCII85Decode":
                out = base64.a85decode(out, adobe=True)
            else:
                raise ValueError(f"Unsupported stream filter: {flt.decode('latin1', 'ignore')}")
        return out

    def _root_object(self) -> int:
        root_match = re.search(rb"/Root\s+(\d+)\s+0\s+R", self.pdf_bytes)
        if root_match:
            return int(root_match.group(1))
        for body in self.objects.values():
            if b"/Type /Catalog" in body:
                num = self._object_number_from_body(body)
                if num is not None:
                    return num
        raise ValueError("PDF catalog/root object not found.")

    def _object_number_from_body(self, body: bytes) -> Optional[int]:
        for obj_num, obj_body in self.objects.items():
            if obj_body is body:
                return obj_num
        return None

    @staticmethod
    def _ref_for_key(body: bytes, key: bytes) -> Optional[int]:
        m = re.search(rb"/" + re.escape(key) + rb"\s+(\d+)\s+0\s+R", body)
        return int(m.group(1)) if m else None

    @staticmethod
    def _refs_in_array_for_key(body: bytes, key: bytes) -> List[int]:
        m = re.search(rb"/" + re.escape(key) + rb"\s*\[(.*?)\]", body, re.S)
        if not m:
            return []
        return [int(x) for x in re.findall(rb"(\d+)\s+0\s+R", m.group(1))]

    def _collect_pages(self) -> List[int]:
        root_obj = self._root_object()
        catalog = self.objects.get(root_obj)
        if not catalog:
            raise ValueError("Catalog object body not found.")
        pages_root = self._ref_for_key(catalog, b"Pages")
        if pages_root is None:
            raise ValueError("Pages root not found in catalog.")

        pages: List[int] = []

        def walk(obj_num: int) -> None:
            body = self.objects.get(obj_num, b"")
            if b"/Type /Pages" in body:
                for kid in self._refs_in_array_for_key(body, b"Kids"):
                    walk(kid)
                return
            if b"/Type /Page" in body:
                pages.append(obj_num)
                return

        walk(pages_root)
        return pages

    @staticmethod
    def _parse_cmap(cmap_data: bytes) -> Dict[int, str]:
        cmap: Dict[int, str] = {}

        def hex_to_unicode(hex_text: bytes) -> str:
            try:
                return bytes.fromhex(hex_text.decode()).decode("utf-16-be", "ignore")
            except Exception:
                return ""

        for section in re.finditer(rb"\d+\s+beginbfchar(.*?)endbfchar", cmap_data, re.S):
            block = section.group(1)
            for src, dst in re.findall(rb"<([0-9A-Fa-f]+)>\s*<([0-9A-Fa-f]+)>", block):
                cmap[int(src, 16)] = hex_to_unicode(dst)

        for section in re.finditer(rb"\d+\s+beginbfrange(.*?)endbfrange", cmap_data, re.S):
            block = section.group(1)
            for line in block.splitlines():
                line = line.strip()
                if not line:
                    continue
                # Form 1: <start> <end> <dst_start>
                m1 = re.match(rb"<([0-9A-Fa-f]+)>\s*<([0-9A-Fa-f]+)>\s*<([0-9A-Fa-f]+)>", line)
                if m1:
                    start = int(m1.group(1), 16)
                    end = int(m1.group(2), 16)
                    dst_text = hex_to_unicode(m1.group(3))
                    if len(dst_text) == 1:
                        base = ord(dst_text)
                        for i, src_code in enumerate(range(start, end + 1)):
                            cmap[src_code] = chr(base + i)
                    continue
                # Form 2: <start> <end> [<dst1> <dst2> ...]
                m2 = re.match(rb"<([0-9A-Fa-f]+)>\s*<([0-9A-Fa-f]+)>\s*\[(.*?)\]", line)
                if m2:
                    start = int(m2.group(1), 16)
                    values = re.findall(rb"<([0-9A-Fa-f]+)>", m2.group(3))
                    for i, dst in enumerate(values):
                        cmap[start + i] = hex_to_unicode(dst)

        return cmap

    def _page_font_map(self, page_obj_num: int) -> Dict[str, FontInfo]:
        page = self.objects.get(page_obj_num, b"")
        resources_ref = self._ref_for_key(page, b"Resources")
        resources = self.objects.get(resources_ref, b"") if resources_ref else page
        font_ref = self._ref_for_key(resources, b"Font")
        if font_ref is None and resources_ref is None:
            # Some PDFs embed resources directly in page objects.
            font_ref = self._ref_for_key(page, b"Font")
        if font_ref:
            font_dict = self.objects.get(font_ref, b"")
        else:
            inline_font = re.search(rb"/Font\s*<<(.*?)>>", resources, re.S)
            font_dict = inline_font.group(1) if inline_font else b""

        fonts: Dict[str, FontInfo] = {}
        for name, font_obj_num in re.findall(rb"/([A-Za-z0-9\+\-]+)\s+(\d+)\s+0\s+R", font_dict):
            font_name = name.decode("latin1")
            font_obj = self.objects.get(int(font_obj_num), b"")
            to_unicode_ref = self._ref_for_key(font_obj, b"ToUnicode")
            cmap: Dict[int, str] = {}
            if to_unicode_ref and to_unicode_ref in self.objects:
                cmap_stream = self._decode_stream(self.objects[to_unicode_ref])
                if cmap_stream:
                    cmap = self._parse_cmap(cmap_stream)
            identity_h = b"/Identity-H" in font_obj
            fonts[font_name] = FontInfo(name=font_name, identity_h=identity_h, cmap=cmap)
        return fonts

    @staticmethod
    def _is_whitespace(b: int) -> bool:
        return b in b" \t\r\n\f\x00"

    @staticmethod
    def _is_delimiter(b: int) -> bool:
        return b in b"()<>[]{}/%"

    @classmethod
    def _parse_literal_string(cls, data: bytes, i: int) -> Tuple[bytes, int]:
        # data[i] == '('
        i += 1
        out = bytearray()
        depth = 1
        while i < len(data):
            c = data[i]
            if c == 0x5C:  # backslash
                i += 1
                if i >= len(data):
                    break
                esc = data[i]
                if esc in b"nrtbf":
                    out.append({ord("n"): 10, ord("r"): 13, ord("t"): 9, ord("b"): 8, ord("f"): 12}[esc])
                    i += 1
                elif esc in b"()\\":
                    out.append(esc)
                    i += 1
                elif esc in b"\r\n":
                    # line continuation
                    if esc == 13 and i + 1 < len(data) and data[i + 1] == 10:
                        i += 2
                    else:
                        i += 1
                elif 48 <= esc <= 55:
                    oct_digits = bytes([esc])
                    i += 1
                    for _ in range(2):
                        if i < len(data) and 48 <= data[i] <= 55:
                            oct_digits += bytes([data[i]])
                            i += 1
                        else:
                            break
                    out.append(int(oct_digits, 8) & 0xFF)
                else:
                    out.append(esc)
                    i += 1
                continue
            if c == 0x28:  # (
                depth += 1
                out.append(c)
                i += 1
                continue
            if c == 0x29:  # )
                depth -= 1
                if depth == 0:
                    i += 1
                    break
                out.append(c)
                i += 1
                continue
            out.append(c)
            i += 1
        return bytes(out), i

    @classmethod
    def _parse_hex_string(cls, data: bytes, i: int) -> Tuple[bytes, int]:
        # data[i] == '<' and data[i+1] != '<'
        i += 1
        hex_chars = bytearray()
        while i < len(data) and data[i] != 0x3E:  # '>'
            if not cls._is_whitespace(data[i]):
                hex_chars.append(data[i])
            i += 1
        if i < len(data) and data[i] == 0x3E:
            i += 1
        if len(hex_chars) % 2 == 1:
            hex_chars.append(ord("0"))
        try:
            return bytes.fromhex(hex_chars.decode("ascii", "ignore")), i
        except Exception:
            return b"", i

    @classmethod
    def _parse_array(cls, data: bytes, i: int) -> Tuple[List[Token], int]:
        # data[i] == '['
        i += 1
        arr: List[Token] = []
        while i < len(data):
            while i < len(data) and cls._is_whitespace(data[i]):
                i += 1
            if i >= len(data):
                break
            if data[i] == 0x25:  # %
                while i < len(data) and data[i] not in b"\r\n":
                    i += 1
                continue
            if data[i] == 0x5D:  # ]
                i += 1
                break
            tok, i = cls._parse_token(data, i)
            if tok is not None:
                arr.append(tok)
        return arr, i

    @classmethod
    def _parse_token(cls, data: bytes, i: int) -> Tuple[Optional[Token], int]:
        while i < len(data) and cls._is_whitespace(data[i]):
            i += 1
        if i >= len(data):
            return None, i
        if data[i] == 0x25:  # %
            while i < len(data) and data[i] not in b"\r\n":
                i += 1
            return None, i

        ch = data[i]
        if ch == 0x28:  # (
            val, i = cls._parse_literal_string(data, i)
            return ("str", val), i
        if ch == 0x3C:  # <
            if i + 1 < len(data) and data[i + 1] == 0x3C:
                return "<<", i + 2
            val, i = cls._parse_hex_string(data, i)
            return ("hex", val), i
        if ch == 0x3E and i + 1 < len(data) and data[i + 1] == 0x3E:
            return ">>", i + 2
        if ch == 0x5B:  # [
            arr, i = cls._parse_array(data, i)
            return arr, i
        if ch == 0x5D:  # ]
            return "]", i + 1
        if ch == 0x2F:  # /
            i += 1
            start = i
            while i < len(data) and (not cls._is_whitespace(data[i])) and data[i] not in b"()<>[]{}%":
                i += 1
            return "/" + data[start:i].decode("latin1", "ignore"), i

        start = i
        while i < len(data) and (not cls._is_whitespace(data[i])) and (not cls._is_delimiter(data[i])):
            i += 1
        return data[start:i].decode("latin1", "ignore"), i

    @classmethod
    def _tokenize(cls, data: bytes) -> List[Token]:
        tokens: List[Token] = []
        i = 0
        while i < len(data):
            tok, i = cls._parse_token(data, i)
            if tok is not None:
                tokens.append(tok)
            else:
                i += 1
        return tokens

    @staticmethod
    def _decode_bytes_with_font(raw: bytes, font: Optional[FontInfo]) -> str:
        if not raw:
            return ""
        if font is None:
            return raw.decode("latin1", "ignore")

        out: List[str] = []
        if font.identity_h:
            if len(raw) % 2 == 1:
                raw = raw + b"\x00"
            codes = [(raw[i] << 8) | raw[i + 1] for i in range(0, len(raw), 2)]
        else:
            codes = list(raw)

        for code in codes:
            mapped = font.cmap.get(code)
            if mapped is not None:
                out.append(mapped)
            elif 0 <= code <= 0x10FFFF:
                out.append(chr(code))
        return "".join(out)

    def _decode_text_token(self, tok: Token, current_font: Optional[FontInfo]) -> str:
        if isinstance(tok, tuple) and tok[0] in {"str", "hex"}:
            return self._decode_bytes_with_font(tok[1], current_font)
        return ""

    def _extract_text_from_content_stream(self, content: bytes, fonts: Dict[str, FontInfo]) -> str:
        tokens = self._tokenize(content)
        stack: List[Token] = []
        current_font: Optional[FontInfo] = None
        parts: List[str] = []

        for tok in tokens:
            if isinstance(tok, str) and tok in {"Tf", "Tj", "TJ", "'", '"', "T*"}:
                if tok == "Tf":
                    # operands: /FontName size
                    size_tok = stack.pop() if stack else None
                    font_tok = stack.pop() if stack else None
                    if isinstance(font_tok, str) and font_tok.startswith("/"):
                        current_font = fonts.get(font_tok[1:])
                    _ = size_tok
                elif tok == "Tj":
                    text_tok = stack.pop() if stack else None
                    text = self._decode_text_token(text_tok, current_font) if text_tok is not None else ""
                    if text:
                        parts.append(text)
                        parts.append("\n")
                elif tok == "TJ":
                    arr_tok = stack.pop() if stack else None
                    if isinstance(arr_tok, list):
                        piece: List[str] = []
                        for item in arr_tok:
                            if isinstance(item, tuple) and item[0] in {"str", "hex"}:
                                piece.append(self._decode_text_token(item, current_font))
                        text = "".join(piece)
                        if text:
                            parts.append(text)
                            parts.append("\n")
                elif tok == "'":
                    text_tok = stack.pop() if stack else None
                    text = self._decode_text_token(text_tok, current_font) if text_tok is not None else ""
                    if text:
                        parts.append(text)
                        parts.append("\n")
                elif tok == '"':
                    # operands: aw ac string
                    text_tok = stack.pop() if stack else None
                    _ = stack.pop() if stack else None
                    _ = stack.pop() if stack else None
                    text = self._decode_text_token(text_tok, current_font) if text_tok is not None else ""
                    if text:
                        parts.append(text)
                        parts.append("\n")
                elif tok == "T*":
                    parts.append("\n")
                continue
            stack.append(tok)

        # Normalize excessive blank lines.
        text = "".join(parts)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    def extract(self) -> str:
        pages = self._collect_pages()
        output: List[str] = []
        for idx, page_obj in enumerate(pages, start=1):
            page_body = self.objects.get(page_obj, b"")
            fonts = self._page_font_map(page_obj)
            content_refs = self._refs_in_array_for_key(page_body, b"Contents")
            single_content = self._ref_for_key(page_body, b"Contents")
            if single_content is not None:
                content_refs = [single_content]
            if not content_refs:
                output.append(f"=== PAGE {idx} ===\n")
                continue
            page_text_parts: List[str] = []
            for content_ref in content_refs:
                content_obj = self.objects.get(content_ref)
                if not content_obj:
                    continue
                stream = self._decode_stream(content_obj)
                if not stream:
                    continue
                txt = self._extract_text_from_content_stream(stream, fonts)
                if txt:
                    page_text_parts.append(txt)
            page_text = "\n".join(part for part in page_text_parts if part)
            output.append(f"=== PAGE {idx} ===\n{page_text}\n")
        return "\n".join(output).strip() + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract text from PDF with basic ToUnicode support.")
    parser.add_argument("pdf", type=Path, help="Input PDF path")
    parser.add_argument("-o", "--output", type=Path, required=True, help="Output .txt path")
    args = parser.parse_args()

    pdf_bytes = args.pdf.read_bytes()
    extractor = PdfExtractor(pdf_bytes)
    text = extractor.extract()
    args.output.write_text(text, encoding="utf-8")
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
