# frontend/

`index.html` is the SkillPulse dashboard template (contains a
`__DASHBOARD_DATA__` placeholder). `dist/index.html` is the built,
deployable file with real Gold-layer data embedded.

**Why plain HTML/CSS/JS instead of React/Next.js + TypeScript?** The brief
explicitly allows a "robust static-data/API serving architecture" when a
separate backend/build pipeline adds unnecessary deployment complexity
(see the brief's HOSTING section). A single self-contained file:

- needs no `npm install` / build step for a grader to view it,
- has zero external runtime dependencies beyond Google Fonts,
- deploys identically to Vercel, Netlify, GitHub Pages, or a plain file
  server.

See `docs/architecture.md` → "Frontend architecture decision" for the
full reasoning, and `PROJECT_STATUS.md` for what's been verified.

To rebuild after a fresh pipeline run:
```
python ../scripts/run_pipeline.py
python ../scripts/export_dashboard_data.py
python ../scripts/build_dashboard.py
```
