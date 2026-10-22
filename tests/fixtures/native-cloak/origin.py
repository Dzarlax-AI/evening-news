"""HTTPS-only fixed-origin fixtures; no arbitrary path or content serving."""

from __future__ import annotations

import argparse
import json
import ssl
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


ROUTES = {
    "/article-alpha": (
        "Fixture signal alpha. Coastal engineers tested a new tide gauge along the northern harbor after winter storms damaged two older sensors. "
        "The replacement unit records water height every thirty seconds and stores a local copy when the radio link drops. "
        "Field technicians compared its readings with a staff gauge during three morning inspections, finding that the largest difference was under two centimeters. "
        "The harbor office will publish another month of observations before deciding whether to install the same equipment near the ferry terminal. "
        "Researchers said the trial is useful because it captures short surges that hourly summaries can miss, while keeping the hardware simple enough for routine maintenance."
    ),
    "/article-beta": (
        "Fixture signal beta. A regional newsroom is testing a shared evening desk to reduce delays when several towns report the same transport disruption. "
        "Editors collect route notices, confirm service changes with operators, and prepare one timeline that local reporters can update from the field. "
        "The pilot began after readers received conflicting messages about a bridge closure and the buses rerouted around it. "
        "During the first week, staff recorded which details needed a second source and how long corrections took to reach the public page. "
        "The desk will continue through the end of the month, when managers plan to compare response times and interview the reporters who used the shared notes."
    ),
    "/article-gamma": (
        "Fixture signal gamma. A neighborhood health team has started mapping shaded walking routes around three primary schools before the warm season. "
        "Volunteers record pavement temperature, tree cover, and places where children can refill water during the afternoon journey home. "
        "The first survey found that two streets with similar traffic had noticeably different surface temperatures because one had a continuous row of mature trees. "
        "School staff will use the map to discuss safer pickup points and to identify crossings that need temporary shade during outdoor events. "
        "The team plans to repeat the measurements on several clear days, then share the data with the city parks office and parent groups."
    ),
    "/article-delta": (
        "Fixture signal delta. Marine researchers have completed a six-week test of low-cost acoustic sensors near a restored eelgrass bed. "
        "The instruments recorded boat noise and animal calls while a separate station logged wind, tide, and water temperature. "
        "Early review suggests that vessel activity was concentrated around the weekend launch window, but the team cautions that weather affected several recordings. "
        "A second analysis will compare the sound samples with routine wildlife counts before researchers publish any conclusions about habitat use. "
        "The project also tests whether volunteer groups can maintain the equipment between visits, a practical requirement for monitoring sites far from the laboratory."
    ),
}

# Longer, varied fixture prose exercises the same article quality checks as a
# publisher page. Each section adds new information rather than repeated filler.
ROUTES["/article-alpha"] += (
    "</p><p>The trial team selected the northern harbor because the channel narrows beside a stone breakwater. "
    "Passing vessels can push brief waves into that corner even when the offshore forecast looks calm. "
    "The old gauges logged only one reading each hour, so staff often learned about those surges from dock workers instead of the instruments. "
    "The new sensor records pressure below the surface and converts it to a water level after accounting for air pressure. "
    "A marked reference pole beside the station lets a technician compare the electronic reading with a direct observation during each visit. "
    "</p><p>Installation took place during a low tide, when the crew could reach the mounting bracket without a boat. "
    "They photographed the cable route, checked the radio antenna from the harbor office, and placed a small battery enclosure above the winter flood mark. "
    "The enclosure stores several days of measurements if the wireless link fails. "
    "When the connection returns, the station sends the missing records in order and marks each one with its original observation time. "
    "This matters to analysts comparing a surge with wind and vessel movements; a delayed upload should never appear to be a new rise in the water. "
    "</p><p>During the first inspections, technicians found a small offset between the sensor and the staff gauge after heavy rain. "
    "They traced it to a reference mark on the bracket and corrected the calibration record without deleting the raw readings. "
    "The published chart will show both the original and corrected series so local engineers can review the change. "
    "The office is also recording the time needed for cleaning, battery checks, and data review. "
    "A device that produces precise numbers but requires frequent emergency visits would be difficult to maintain at a remote ferry landing. "
    "</p><p>Harbor staff plan to compare the station with a second temporary gauge during spring tides. "
    "If the instruments agree, they will move that temporary unit to the ferry terminal for another month. "
    "The decision will consider data quality, maintenance time, and whether the shorter sampling interval reveals conditions that matter to navigation. "
    "Researchers will share their method with neighboring ports, including the calibration notes and gaps caused by lost radio coverage. "
    "They caution that one winter trial cannot establish a long-term sea-level trend, but it can show whether the equipment is dependable enough to collect one."
)
ROUTES["/article-beta"] += (
    "</p><p>The shared desk receives notices from bus operators, road crews, and emergency services, then checks each claim against a second source before updating its timeline. "
    "A report about a closed junction may be accurate for one direction and outdated for the other, so editors record the location, direction, and observation time together. "
    "Local reporters can add photographs or eyewitness accounts, but those notes remain separate from confirmed service changes until an editor verifies them. "
    "The arrangement lets readers see what is known while avoiding a stream of contradictory alerts. "
    "</p><p>On the first morning of the pilot, a stalled truck blocked the bridge approach during school traffic. "
    "One operator announced a diversion before another had decided where its vehicles could turn around. "
    "The desk published the first confirmed route change and labeled the remaining service as awaiting confirmation. "
    "A reporter at the station checked the temporary stop locations, while a colleague called the transport office about accessible boarding. "
    "Within an hour, the timeline included both the route map and a clear note about the stop that wheelchairs could still reach. "
    "</p><p>Editors are keeping an internal log of every revision, including the source and the reason for a correction. "
    "The log helps distinguish a genuine change on the road from an earlier reporting mistake. "
    "It also shows whether an update reached the local pages promptly after the central desk confirmed it. "
    "Each newsroom retains control of its own headline and local context; the common timeline supplies verified facts that several towns need at once. "
    "Reporters say this saves time when they are working from a crowded junction with limited phone service. "
    "</p><p>At the end of the month, managers will review the revision log with staff from every participating newsroom. "
    "They will look for gaps in coverage, delays caused by unclear handoffs, and cases where a local detail should have stayed on a town page. "
    "The pilot has not yet shown whether a permanent shared desk would cost less or publish faster across all disruptions. "
    "The review will include quiet days as well as major closures, because maintaining a desk when little is happening is part of the operating cost. "
    "Readers will be asked whether the combined timeline made changes easier to follow."
)
ROUTES["/article-gamma"] += (
    "</p><p>The walking survey follows routes that families already use, rather than drawing a theoretical shortest path between each school and its surrounding homes. "
    "Volunteers begin after lunch and take measurements at the same marked points on every visit. "
    "They note the weather, the type of pavement, nearby walls, and whether a tree or awning shades the ground at that hour. "
    "A shaded corner can feel comfortable in the morning yet be exposed when children leave school, so the afternoon timing matters. "
    "</p><p>At the first school, the team compared two parallel streets with similar traffic. "
    "The street lined with mature trees offered shade across most of the pavement, while the open street had a long stretch beside a bare wall. "
    "The volunteers did not treat one set of readings as proof that every day will be the same. "
    "They scheduled repeat visits on clear and cloudy afternoons and will record any temporary shade from parked vehicles or construction fencing. "
    "The comparison gives school staff a starting point for asking where small changes could help. "
    "</p><p>Water access is another part of the map. "
    "A public fountain near one route was working during the survey, but the volunteers found that its opening hours differed from the adjacent sports center. "
    "They confirmed the fountain's maintenance contact and marked the nearest indoor alternative in case it closes. "
    "The map also identifies crossings where families wait in direct sunlight for several traffic-light cycles. "
    "Staff will discuss those waits with the city traffic office before proposing any temporary canopy or permanent street work. "
    "</p><p>The team plans to share draft maps with parents and older pupils before publishing them. "
    "Families may know a safe shortcut or a difficult crossing that a daytime survey missed. "
    "Their comments will be checked on foot, then added with a date so the map does not imply that every water point or shaded area is permanent. "
    "School staff will use the findings to review pickup guidance during hot weather and to plan outdoor events. "
    "The parks office will receive the measurements and photos, while the health team will repeat the survey later in the season to see how the routes change."
)
ROUTES["/article-delta"] += (
    "</p><p>The acoustic stations sit outside the densest part of the eelgrass bed, where their anchors are less likely to disturb new shoots. "
    "Each unit records a short sample at regular intervals and stores the sound locally until a researcher retrieves the card. "
    "A separate weather station records wind and rain so analysts can identify periods when surface noise may mask quieter animal calls. "
    "The team also keeps a maintenance log for battery changes, sensor cleaning, and any movement of an anchor after rough weather. "
    "</p><p>Weekend recordings contain more engine noise near the public launch, but the researchers have not yet counted the vessels or identified their routes. "
    "They will compare the sound with harbor traffic records and direct observations before assigning a source to each event. "
    "Some low-frequency signals could come from wind moving the moorings rather than a boat passing the site. "
    "The team flags uncertain segments for manual review instead of treating the detection software as a final answer. "
    "That review is slower, but it protects the wildlife analysis from a false pattern. "
    "</p><p>Wildlife volunteers visit the restoration area on a separate schedule to count fish and invertebrates along fixed transects. "
    "Those counts provide context for the sound recordings but do not turn a call into a confirmed sighting. "
    "Researchers will compare several weeks of observations before asking whether noisy periods correspond with a change in animal activity. "
    "They also want to know whether the sensors remain useful during stormy weather, when the bed is hardest to inspect directly. "
    "A failed station will be marked as missing data, never filled with a modeled value in the public record. "
    "</p><p>The six-week pilot is as much a test of field maintenance as it is an ecological study. "
    "Volunteer groups may eventually replace batteries and inspect housings at distant sites, but they need clear instructions and a safe way to report damage. "
    "The project team is preparing illustrated maintenance notes and a checklist based on the problems seen during this first deployment. "
    "A later report will describe sensor performance, coverage gaps, and the limits of the wildlife comparison. "
    "For now, the recordings show when the soundscape changed; they do not establish whether the restored eelgrass has altered animal behavior."
)


def handler_for(proof_path: str | None):
    class Handler(BaseHTTPRequestHandler):
        server_version = "NativeFixture/1"
        sys_version = ""

        def log_message(self, _format, *_args):
            return

        def _proof(self, route: str):
            if proof_path:
                row = {"fixture_id": route, "tls_version": self.connection.version(),
                       "method": self.command, "status": self._status}
                with open(proof_path, "a", encoding="utf-8") as stream:
                    stream.write(json.dumps(row, sort_keys=True) + "\n")

        def _send(self, status: int, body: bytes, content_type="text/html; charset=utf-8", headers=()):
            self._status = status
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            for key, value in headers:
                self.send_header(key, value)
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)

        def do_GET(self):
            route = self.path.split("?", 1)[0]
            if route == "/redirect":
                self._send(302, b"", headers=(("Location", "/article-beta"),))
                self._proof("redirect")
                return
            if route == "/cookie-proof":
                ok = "native_cookie=present" in self.headers.get("Cookie", "")
                body = b"cookie confirmed" if ok else b"cookie missing"
                self._send(200 if ok else 403, body, "text/plain; charset=utf-8")
                self._proof("cookie-proof")
                return
            if route == "/js-article":
                story = json.dumps(ROUTES["/article-alpha"])
                body = ("<!doctype html><meta charset=utf-8><title>Fixture alpha</title>"
                        "<main id=article></main><script>"
                        "document.cookie='native_cookie=present; Path=/; SameSite=Lax';"
                        "fetch('/cookie-proof').then(r=>r.text()).then(t=>{"
                        f"document.querySelector('#article').innerHTML='<article data-ready=\"true\"><h1>Fixture alpha</h1><p>'+{story}+'</p><p>'+t+'</p></article>';"
                        "});</script>").encode()
                self._send(200, body)
                self._proof("article-js-cookie")
                return
            if route == "/slow":
                time.sleep(0.35)
                body = self._article("gamma")
                self._send(200, body)
                self._proof("slow-article")
                return
            if route == "/huge-dom":
                text = ROUTES["/article-delta"]
                padding = "deterministic fixture padding " * 7500
                body = ("<!doctype html><meta charset=utf-8><title>Large fixture</title>"
                        "<script type=application/json id=fixture-padding>" + padding + "</script>"
                        "<main data-ready=\"true\"><article><h1>Large fixture</h1><p>" +
                        text + "</p></article></main>").encode()
                self._send(200, body)
                self._proof("large-dom")
                return
            if route == "/challenge":
                body = b"<!doctype html><html><head><title>Just a moment</title><script src='/cdn-cgi/challenge-platform/cf_chl_x'></script></head><body data-ready=\"true\"><form id=\"challenge-form\">Synthetic challenge</form></body></html>"
                self._send(200, body)
                self._proof("challenge")
                return
            if route == "/pixel.svg":
                self._send(200, b"<svg xmlns='http://www.w3.org/2000/svg' width='1' height='1'/>", "image/svg+xml")
                self._proof("subresource")
                return
            key = {"/article-beta": "beta"}.get(route)
            if key:
                self._send(200, self._article(key))
                self._proof("article-beta")
                return
            self._send(404, b"fixture route rejected", "text/plain; charset=utf-8")
            self._proof("not-found")

        @staticmethod
        def _article(key):
            return ("<!doctype html><html><head><title>Fixture article</title><meta property='og:title' content='Fixture article'></head>"+
                    f"<body><article data-ready=\"true\"><h1>Fixture {key}</h1><p>{ROUTES['/article-' + key]}</p><img src='/pixel.svg' alt='fixture'></article></body></html>").encode()

    return Handler


def serve(host: str, port: int, cert: str, key: str, proof_path: str | None = None):
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.load_cert_chain(cert, key)
    server = ThreadingHTTPServer((host, port), handler_for(proof_path))
    server.socket = context.wrap_socket(server.socket, server_side=True)
    server.serve_forever()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=443)
    parser.add_argument("--cert", required=True)
    parser.add_argument("--key", required=True)
    parser.add_argument("--proof-output")
    args = parser.parse_args(argv)
    serve(args.host, args.port, args.cert, args.key, args.proof_output)


if __name__ == "__main__":
    main()
