"""Enqueue a synthetic news_digest through the production Python job producer."""

import asyncio
import json
import re
import sys


async def enqueue(payload, key):
    from news_aggregator.database import AsyncSessionLocal
    from news_aggregator.services.background_jobs import enqueue_job_in_session

    async with AsyncSessionLocal() as session:
        job = await enqueue_job_in_session(session, "news_digest", payload, key, coalesce_active=False)
        await session.commit()
        return str(job.id)


def main():
    if len(sys.argv) != 3 or not re.fullmatch(r"go-news-digest-default-[0-9a-f]{12}", sys.argv[2]):
        raise SystemExit(2)
    try:
        payload = json.loads(sys.argv[1])
    except (ValueError, TypeError):
        raise SystemExit(2) from None
    if not isinstance(payload, dict) or payload:
        raise SystemExit(2)
    job_id = asyncio.run(enqueue(payload, sys.argv[2]))
    print(json.dumps({"job_id": job_id}, separators=(",", ":")))


if __name__ == "__main__":
    main()
