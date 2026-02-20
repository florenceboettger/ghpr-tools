import argparse
import calendar
import inspect
import json
import logging
import os
from pathlib import Path
import re
import requests
import signal
import sys
import time

_base_url = 'https://api.github.com/'
_pulls_url_template = _base_url + 'repos/{owner}/{repo}/pulls?state=all&sort=created&direction=asc&per_page={per_page}&page={page}'
_issues_url_template = _base_url + 'repos/{owner}/{repo}/issues?state=all&sort=created&direction=asc&per_page={per_page}&page={page}'
_pull_url_template = _base_url + 'repos/{owner}/{repo}/pulls/{pull_number}'
_issue_url_template = _base_url + 'repos/{owner}/{repo}/issues/{issue_number}'

_repo_path_template = os.path.join('{dst_dir}', '{owner}', '{repo}')
_pulls_path_template = os.path.join('{dst_dir}', '{owner}', '{repo}', 'pulls-page-{page}.json')
_pull_path_template = os.path.join('{dst_dir}', '{owner}', '{repo}', 'pull-{pull_number}.json')
_issue_path_template = os.path.join('{dst_dir}', '{owner}', '{repo}', 'issue-{issue_number}.json')
_diff_path_template = os.path.join('{dst_dir}', '{owner}', '{repo}', 'pull-{pull_number}.diff')

_linked_issues_pattern_template = r'\b(?:close|closes|closed|fix|fixes|fixed|resolve|resolves|resolved)\s+(?:https://github\.com/{owner}/{repo}/issues/|{owner}/{repo}#|#)(\d+)\b'

_last_page_pattern = re.compile(r'page=(\d+)>; rel="last"')

def _make_linked_issues_regex(owner, repo):
    owner = owner.replace('.', r'\.')
    repo = repo.replace('.', r'\.')
    pattern = _linked_issues_pattern_template.format(owner=owner, repo=repo)
    return re.compile(pattern, flags=re.IGNORECASE)

def _extract_linked_issue_numbers(pull_body, linked_issues_regex):
    if pull_body is None:
        return []
    return [int(n) for n in linked_issues_regex.findall(pull_body)]

def _save_json(obj, path):
    with open(path, 'w') as f:
        json.dump(obj, f, indent=2, sort_keys=True)

def _save_txt(txt, path):
    with open(path, 'wb') as f:
        f.write(txt)

def _iso_to_unix(iso):
    utc_time = time.strptime(iso, '%Y-%m-%dT%H:%M:%SZ')
    return calendar.timegm(utc_time)

def _ensure_dir_exists(path):
    Path(path).mkdir(parents=True, exist_ok=True)

class TooManyRequestFailures(Exception):
    pass

class Crawler(object):
    """Crawl GitHub repositories to find and save merged pull requests and the issues
    they have fixed.

    The crawler goes through the pages of closed pull requests, from oldest to
    newest. If a pull request is merged and links one or more issues in its
    description, the pull request and its linked issue(s) will be fetched and
    saved as JSON files. The list of linked issue numbers is added to the fetched
    pull request JSON object with the key "linked_issue_numbers". The JSON files
    will be saved in DEST_DIR/owner/repo. The directories will be created if they
    do not already exist. The naming pattern for files is issue-N.json for issues,
    pull-N.json for pull requests, and pulls-page-N.json for pages of pull
    requests. Any existing file will be overwritten. The GitHub API limits
    unauthenticated clients to 60 requests per hour. The rate limit is 5,000
    requests per hour for authenticated clients. For this reason, you should
    provide a GitHub OAuth token if you want to crawl a large repository. You can
    create a personal access token at https://github.com/settings/tokens.

    Attributes:
        dst_dir (str): Directory for saving JSON files.
        per_page (int): Pull requests per page, between 1 and 100.
        save_pull_pages (bool): Save the pages of pull requests.
        max_request_tries (int): Number of times to try a request before
            terminating.
        request_retry_wait_secs (int): Seconds to wait before retrying a failed request.
        max_pull_number (int): Maximun number of pull requests to crawl.
    """

    def __init__(self,
                 token=None,
                 dst_dir='repos',
                 per_page=100,
                 save_pull_pages=False,
                 max_request_tries=100,
                 request_retry_wait_secs=10,
                 max_issue_number=-1,
                 start_date="2000-01-01",
                 end_date="2050-01-01"):
        """Initializes Crawler.

        The GitHub API limits unauthenticated clients to 60 requests per hour. The
        rate limit is 5,000 requests per hour for authenticated clients. For this
        reason, you should provide a GitHub OAuth token if you want to crawl a large
        repository. You can create a personal access token at
        https://github.com/settings/tokens.

        Args:
            token (str): Your GitHub OAuth token. If None, the crawler will be
                unauthenticated.
            dst_dir (str): Directory for saving JSON files.
            per_page (int): Pull requests per page, between 1 and 100.
            save_pull_pages (bool): Save the pages of pull requests.
            max_request_tries (int): Number of times to try a request before
                terminating.
            request_retry_wait_secs (int): Seconds to wait before retrying a failed request.
        """
        self.dst_dir = dst_dir
        self.per_page = per_page
        self.save_pull_pages = save_pull_pages
        self.max_request_tries = max_request_tries
        self.request_retry_wait_secs = request_retry_wait_secs
        self.start_date = _iso_to_unix(start_date + "T00:00:00Z")
        self.end_date = _iso_to_unix(end_date + "T00:00:00Z")
        self._headers = {
            'Accept': 'application/vnd.github.v3+json',
        }
        if token is not None:
            self._headers['Authorization'] = 'token ' + token
        self._max_issue_number = max_issue_number
        self._interrupted = False
        def sigint_handler(signal, frame):
            if self._interrupted:
                print('\nForced exit')
                sys.exit(2)
            self._interrupted = True
            print('\nInterrupted, finishing current page\nPress interrupt key again to force exit')
        signal.signal(signal.SIGINT, sigint_handler)

    def crawl(self,
              owner,
              repo,
              start_page_pulls=-1, # 478
              start_page_issues=-1, # 2701
              end_page_pulls=-1,
              end_page_issues=-1):
        """Crawls a GitHub repository, finds and saves merged pull requests and the issues
        they have fixed.

        The crawler goes through the pages of closed pull requests, from oldest to
        newest. If a pull request is merged and links one or more issues in its
        description, the pull request and its linked issue(s) will be fetched and
        saved as JSON files. The list of linked issue numbers is added to the fetched
        pull request JSON object with the key "linked_issue_numbers". The JSON files
        will be saved in DEST_DIR/owner/repo. The directories will be created if they
        do not already exist. The naming pattern for files is issue-N.json for issues,
        pull-N.json for pull requests, and pulls-page-N.json for pages of pull
        requests. Any existing file will be overwritten.

        Args:
            owner (str): The username of the repository owner, e.g., "octocat" for the
                https://github.com/octocat/Hello-World repository.
            repo (str): The name of the repository, e.g., "Hello-World" for the
                https://github.com/octocat/Hello-World repository.
            start_page (int): Page to start crawling from.

        Raises:
            TooManyRequestFailures: A request failed max_request_tries times.
        """

        pulls_url = _pulls_url_template.format(per_page=self.per_page, owner=owner, repo=repo, page='{page}')
        issues_url = _issues_url_template.format(per_page=self.per_page, owner=owner, repo=repo, page='{page}')

        if start_page_pulls < 1:
            if self.start_date < _iso_to_unix("2015-01-01T00:00:00Z"):
                logging.info('Starting date before 2015. Pulls starting at page 1.')
                print('Starting date before 2015. Pulls starting at page 1.')
                start_page_pulls = 1
            logging.info('Getting starting pull page.')
            print('Getting starting pull page.')
            start_page_pulls = self._get_starting_page(pulls_url)
            logging.info('Pulls starting page found: {}'.format(start_page_pulls))
            print('Pulls starting page: {}'.format(start_page_pulls))
        if start_page_issues < 1:
            if self.start_date < _iso_to_unix("2015-01-01T00:00:00Z"):
                logging.info('Starting date before 2015. Issues starting at page 1.')
                print('Starting date before 2015. Issues starting at page 1.')
                start_page_issues = 1
            logging.info('Getting starting issues page.')
            print('Getting starting issues page.')
            start_page_issues = self._get_starting_page(issues_url)
            logging.info('Issues starting page found: {}'.format(start_page_issues))
            print('Issues starting page: {}'.format(start_page_issues))

        if end_page_pulls < 1:
            if self.end_date > time.time():
                logging.info('Ending date after current time. Pulls ending at page -1')
                print('Ending date after current time. Pulls ending at page -1.')
                end_page_pulls = -1
            else:
                logging.info('Getting ending pull page.')
                print('Getting ending pull page.')
                pulls_url = _pulls_url_template.format(per_page=self.per_page, owner=owner, repo=repo, page='{page}')
                end_page_pulls = self._get_ending_page(pulls_url)
                logging.info('Pulls ending page found: {}'.format(end_page_pulls))
                print('Pulls ending page: {}'.format(end_page_pulls))
        if end_page_issues < 1:
            if self.end_date > time.time():
                logging.info('Ending date after current time. Issues ending at page -1.')
                print('Ending date after current time. Issues ending at page -1.')
                end_page_issues = -1
            else:
                logging.info('Getting ending issues page.')
                print('Getting ending issues page.')
                end_page_issues = self._get_ending_page(issues_url)
                logging.info('Issues ending page found: {}'.format(end_page_issues))
                print('Issues ending page: {}'.format(end_page_issues))

        _ensure_dir_exists(_repo_path_template.format(dst_dir=self.dst_dir, owner=owner, repo=repo))
        linked_issues_regex = _make_linked_issues_regex(owner, repo)
        page = start_page_pulls
        num_issues = 0
        num_pulls = 0
        list_issues = {}
        self._interrupted = False
        logging.info('Pulls crawl: starting {} {}/{}'.format(start_page_pulls, owner, repo))
        print('Pulls: Starting from page {} ({}/{})'.format(start_page_pulls, owner, repo))
        while not self._interrupted and not (self._max_issue_number > 0 and num_issues >= self._max_issue_number) and not (end_page_pulls > 0 and page > end_page_pulls):
            pulls, ok = self._get_json(_pulls_url_template.format(per_page=self.per_page, owner=owner, repo=repo, page=page))
            if not ok:
                continue
            if self.save_pull_pages:
                _save_json(pulls, _pulls_path_template.format(dst_dir=self.dst_dir, owner=owner, repo=repo, page=page))
            for p in pulls:
                if _iso_to_unix(p['created_at']) < self.start_date or _iso_to_unix(p['created_at']) > self.end_date:
                    continue
                linked_issue_numbers = _extract_linked_issue_numbers(p.get('body'), linked_issues_regex)
                pull_number = p['number']
                pull, ok = self._get_json(_pull_url_template.format(owner=owner, repo=repo, pull_number=pull_number))
                if not ok:
                    continue
                pull['linked_issue_numbers'] = linked_issue_numbers
                diff_url = pull['diff_url']
                diff, ok = self._get(diff_url)
                if not ok:
                    continue
                _save_txt(diff.content, _diff_path_template.format(dst_dir=self.dst_dir, owner=owner, repo=repo, pull_number=pull_number))
                _save_json(pull, _pull_path_template.format(dst_dir=self.dst_dir, owner=owner, repo=repo, pull_number=pull_number))
                for issue_number in linked_issue_numbers:
                    issue, ok = self._get_json(_issue_url_template.format(owner=owner, repo=repo, issue_number=issue_number))
                    if not ok:
                        continue
                    _save_json(issue, _issue_path_template.format(dst_dir=self.dst_dir, owner=owner, repo=repo, issue_number=issue_number))
                    num_issues += 1
                    list_issues[issue_number] = True
                num_pulls += 1
                if self._max_issue_number > 0 and num_issues >= self._max_issue_number:
                    break
            logging.info('Pulls crawl: finished {}, saved {} issues and {} pull requests ({}/{})'.format(page, num_issues, num_pulls, owner, repo))
            print('Pulls page {} finished, saved {} issues and {} pull requests ({}/{})'.format(page, num_issues, num_pulls, owner, repo))
            if len(pulls) < self.per_page or (self._max_issue_number > 0 and num_issues >= self._max_issue_number):
                logging.info('Pulls crawl: finished all, {} issues {} pulls ({}/{})'.format(num_issues, num_pulls, owner, repo))
                print('Pulls all pages finished, saved {} issues and {} pull requests ({}/{})'.format(num_issues, num_pulls, owner, repo))
                if self._interrupted:
                    return
                break
            page += 1

        page = start_page_issues
        num_issues = 0
        logging.info('Issues crawl: starting {} {}/{}'.format(start_page_issues, owner, repo))
        print('Issues: Starting from page {} ({}/{})'.format(start_page_issues, owner, repo))
        while not self._interrupted and not (self._max_issue_number > 0 and num_issues >= self._max_issue_number) and not (end_page_issues > 0 and page > end_page_issues):
            issues, ok = self._get_json(_issues_url_template.format(per_page=self.per_page, owner=owner, repo=repo, page=page))
            if not ok:
                continue
            for i in issues:
                issue_number = i['number']
                if issue_number in list_issues or _iso_to_unix(i['created_at']) < self.start_date or _iso_to_unix(i['created_at']) > self.end_date:
                    continue
                issue, ok = self._get_json(_issue_url_template.format(owner=owner, repo=repo, issue_number=issue_number))
                if not ok:
                    continue
                _save_json(issue, _issue_path_template.format(dst_dir=self.dst_dir, owner=owner, repo=repo, issue_number=issue_number))
                num_issues += 1
                if self._max_issue_number > 0 and num_issues >= self._max_issue_number:
                    break
            logging.info('Issues crawl: finished {}, saved {} issues ({}/{})'.format(page, num_issues, owner, repo))
            print('Issues page {} finished, saved {} issues ({}/{})'.format(page, num_issues, owner, repo))
            if len(issues) < self.per_page or (self._max_issue_number > 0 and num_issues >= self._max_issue_number):
                logging.info('Issues crawl: finished all, {} issues ({}/{})'.format(num_issues, owner, repo))
                print('Issues all pages finished, saved {} issues ({}/{})'.format(num_issues, owner, repo))
                if self._interrupted:
                    return
                break
            page += 1

    def _get_starting_page(self, url):
        r, ok = self._get(url.format(page=1))
        link = r.headers['Link']
        last_page = int(_last_page_pattern.findall(link)[0])
        start_page = self._find_start_page(url, 1, last_page)
        return start_page

    def _find_start_page(self, url, start, end):
        mid = round((start + end)/2)        
        print('querying at {} between {} and {}'.format(mid, start, end))
        pulls, ok = self._get_json(url.format(page=mid))
        first_date = _iso_to_unix(pulls[0]['created_at'])
        last_date = _iso_to_unix(pulls[-1]['created_at'])

        if start == end:
            print('first: {}, last: {}, goal: {}'.format(first_date, last_date, self.start_date))
            if self.start_date > last_date:
                return mid + 1
            return mid

        if self.start_date < first_date:
            return self._find_start_page(url, start, mid)
        if self.start_date > last_date:
            return self._find_start_page(url, mid, end)
        return mid

    def _get_ending_page(self, url):
        r, ok = self._get(url.format(page=1))
        link = r.headers['Link']
        last_page = int(_last_page_pattern.findall(link)[0])
        start_page = self._find_end_page(url, 1, last_page)
        return start_page

    def _find_end_page(self, url, start, end):
        mid = round((start + end)/2)
        print('querying at {} between {} and {}'.format(mid, start, end))
        pulls, ok = self._get_json(url.format(page=mid))
        first_date = _iso_to_unix(pulls[0]['created_at'])
        last_date = _iso_to_unix(pulls[-1]['created_at'])

        if start == end:
            print('first: {}, last: {}, goal: {}'.format(first_date, last_date, self.start_date))
            if self.end_date < first_date:
                return mid - 1
            return mid

        if self.end_date < first_date:
            return self._find_end_page(url, start, mid)
        if self.end_date > last_date:
            return self._find_end_page(url, mid, end)
        return mid

    def _get_json(self, url):
        r, ok = self._get(url)
        if ok:
            return r.json(), ok
        else:
            return {}, ok


    def _get(self, url):
        tries = 0
        while True:
            r, ok = self._try_to_get(url)
            if r is not None:
                return r, ok
            tries += 1
            if tries >= self.max_request_tries:
                print('Request failed {} times, aborting'.format(tries))
                raise TooManyRequestFailures('{} request failures for {}'.format(tries, url))
            print('Request failed {} times, retrying in {} seconds'.format(tries, self.request_retry_wait_secs))
            time.sleep(self.request_retry_wait_secs)

    def _try_to_get(self, url):
        try:
            r = requests.get(url, headers=self._headers)
            if not r.ok:
                logging.error('Get: not ok: {} {} {} {}'.format(url, r.status_code, r.headers, r.text))
                if r.status_code == 404:
                    return {}, False
                if 'X-Ratelimit-Remaining' in r.headers and int(r.headers['X-Ratelimit-Remaining']) < 1 and 'X-Ratelimit-Reset' in r.headers:
                    ratelimit_wait_secs = int(r.headers['X-Ratelimit-Reset']) - int(time.time()) + 1
                    logging.info('Get: waiting {} secs for rate limit reset'.format(ratelimit_wait_secs))
                    print('Rate limit reached, waiting {} secs for reset'.format(ratelimit_wait_secs))
                    time.sleep(ratelimit_wait_secs)
                    return self._try_to_get(url)
                return None, False
        except Exception as e:
            logging.error('Get: exception: {} {}'.format(url, e))
            return None
        if 'Content-Type' in r.headers and 'json' in r.headers['Content-Type'] and isinstance(r.json(), dict) and 'message' in r.json():
            logging.error('Get: error: {} {}'.format(url, r.json()))
            return None
        return r, True

def main():
    init_params = inspect.signature(Crawler.__init__).parameters
    crawl_params = inspect.signature(Crawler.crawl).parameters
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description='Crawl GitHub repositories to find and save merged pull requests and the issues they have fixed. '
                    'The crawler goes through the pages of closed pull requests, from oldest to newest. '
                    'If a pull request is merged and links one or more issues in its description, '
                    'the pull request and its linked issue(s) will be fetched and saved as JSON files. '
                    'The list of linked issue numbers is added to the fetched pull request JSON object with the key "linked_issue_numbers". '
                    'The JSON files will be saved in DEST_DIR/owner/repo. '
                    'The directories will be created if they do not already exist. '
                    'The naming pattern for files is issue-N.json for issues, pull-N.json for pull requests, '
                    'and pulls-page-N.json for pages of pull requests. '
                    'Any existing file will be overwritten. '
                    'The GitHub API limits unauthenticated clients to 60 requests per hour. '
                    'The rate limit is 5,000 requests per hour for authenticated clients. '
                    'For this reason, you should provide a GitHub OAuth token if you want to crawl a large repository. '
                    'You can create a personal access token at https://github.com/settings/tokens.')
    parser.add_argument('-t', '--token', type=str, default=init_params['token'].default,
        help='your GitHub OAuth token, can also be provided via a GITHUB_OAUTH_TOKEN environment variable')
    parser.add_argument('-d', '--dst-dir', type=str, default=init_params['dst_dir'].default,
        help='directory for saving JSON files')
    parser.add_argument('-p', '--start-page-pulls', type=int, default=crawl_params['start_page_pulls'].default,
        help='pulls page to start crawling from')
    parser.add_argument('-i', '--start-page-issues', type=int, default=crawl_params['start_page_issues'].default,
        help='issues page to start crawling from')
    parser.add_argument('-P', '--end-page-pulls', type=int, default=crawl_params['end_page_pulls'].default,
        help='pulls page to stop crawling at')
    parser.add_argument('-I', '--end-page-issues', type=int, default=crawl_params['start_page_issues'].default,
        help='issues page to stop crawling at')
    parser.add_argument('--per-page', type=int, default=init_params['per_page'].default,
        help='pull requests per page, between 1 and 100')
    parser.add_argument('-a', '--save-pull-pages', action='store_true',
        help='save the pages of pull requests')
    parser.add_argument('-m', '--max-request-tries', type=int, default=init_params['max_request_tries'].default,
        help='number of times to try a request before terminating')
    parser.add_argument('-r', '--request-retry-wait-secs', type=int, default=init_params['request_retry_wait_secs'].default,
        help='seconds to wait before retrying a failed request')
    parser.add_argument('-l', '--log-file', type=str, default=None,
        help='file to write logs to')
    parser.add_argument('-n', '--max-issue-number', type=int, default=init_params['max_issue_number'].default,
        help='maximum number of issues to crawl')
    parser.add_argument('-e', '--start-date', type=str, default=init_params['start_date'].default,
        help='date from which to start the crawl, with pattern YYYY-MM-DD')
    parser.add_argument('-E', '--end-date', type=str, default=init_params['end_date'].default,
        help='date at which to end the crawl, with pattern YYYY-MM-DD')
    parser.add_argument('repos', metavar='repo', type=str, nargs='+',
        help='full repository name, e.g., "octocat/Hello-World" for the https://github.com/octocat/Hello-World repository')
    args = parser.parse_args()

    if args.token is None:
        args.token = os.environ.get('GITHUB_OAUTH_TOKEN')
    if args.token == '':
        args.token = None

    if args.log_file is not None:
        logging.basicConfig(filename=args.log_file,
                            filemode='w',
                            format='[%(asctime)s] %(levelname)s: %(message)s',
                            datefmt='%Y-%m-%d %H:%M',
                            level=logging.DEBUG)

    crawler = Crawler(token=args.token,
                      dst_dir=args.dst_dir,
                      per_page=args.per_page,
                      save_pull_pages=args.save_pull_pages,
                      max_request_tries=args.max_request_tries,
                      request_retry_wait_secs=args.request_retry_wait_secs,
                      max_issue_number=args.max_issue_number,
                      start_date=args.start_date,
                      end_date=args.end_date)
    for r in args.repos:
        n = r.find('/')
        owner = r[:n]
        repo = r[n+1:]
        try:
            crawler.crawl(owner,
                          repo,
                          start_page_pulls=args.start_page_pulls,
                          start_page_issues=args.start_page_issues,
                          end_page_pulls=args.end_page_pulls,
                          end_page_issues=args.end_page_issues)
        except Exception as e:
            logging.error('Main: exception: {}/{} {}'.format(owner, repo, e))
            print('Terminated with error: {} ({}/{})'.format(e, owner, repo))

if __name__ == '__main__':
    main()
