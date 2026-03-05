# Social Media Playbook (LinkedIn + GitHub)

This project includes a ready-to-publish social media kit generated from the latest project snapshots.

## Generated Assets

- LinkedIn portrait videos (9:16):
  - `social_media/videos/linkedin_showcase_en.mp4`
  - `social_media/videos/linkedin_showcase_tr.mp4`
- GitHub/desktop landscape videos (16:9):
  - `social_media/videos/github_showcase_en.mp4`
  - `social_media/videos/github_showcase_tr.mp4`
- Slide PNG sets (for carousel posts):
  - `social_media/slides/en_portrait/`
  - `social_media/slides/tr_portrait/`
  - `social_media/slides/en_landscape/`
  - `social_media/slides/tr_landscape/`
- LinkedIn caption drafts:
  - `social_media/captions/linkedin_post_en.md`
  - `social_media/captions/linkedin_post_tr.md`

## Recommended Posting Flow

1. LinkedIn post (EN or TR) with `linkedin_showcase_*.mp4`.
2. Add first comment with repo link and one specific metric highlight.
3. GitHub README/release update with `github_showcase_en.mp4`.
4. Optional second LinkedIn carousel using `social_media/slides/*_portrait/`.

## Best Upload Settings

- LinkedIn video: MP4, 1080x1920, <= 30s (current assets are ~25s).
- GitHub video: MP4, 1920x1080.
- Keep cover image as `slide_01_cover.png` for visual consistency.

## Regenerate Kit

```bash
make social-kit
```

or:

```bash
python3 scripts/generate_social_media_kit.py
```

## Notes

- Social visuals are generated from project metrics and architecture; no secret payloads are embedded.
- If snapshot metrics change, regenerate before posting to keep numbers current.
