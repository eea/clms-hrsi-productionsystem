#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from si_geometry.geometry_functions import *
import multiprocessing


copernicus_hub_dict = {'user': 'hidden_value', 'password': 'hidden_value'}


def get_single_shape_wkt(input_shapefile):
    shapes = []
    with fiona.open(input_shapefile) as ds:
        proj_in = ds.meta['crs_wkt']
        for inv in list(ds.items()):
            shapes.append(shape(inv[1]['geometry']))
    assert len(shapes) == 1
    return shapes[0].wkt


def multiple_split(txt, split_list):
    out = [txt]
    for split_item in split_list:
        out_new = []
        for el in out:
            out_new += el.split(split_item)
        out = [el for el in out_new if len(el) > 0]
    return out


def htlmline_to_keyvalue(str_in):
    parts = multiple_split(str_in, ['<', '>'])
    if len(parts) != 4:
        return None, None
    for el in ['title', 'id', 'summary']:
        if parts[0] == el:
            return el, parts[1]
    if not 'name' in parts[0]:
        return None, None
    type_val = parts[0].split()[0]
    key = parts[0].split('name=')[-1].replace('"', '')
    value = parts[1]
    if type_val in ['float', 'double']:
        value = float(value)
    elif type_val in ['int']:
        value = int(value)
    elif type_val == 'date':
        try:
            if '.' in value:
                value = datetime.strptime(value[0:-1] + '000', '%Y-%m-%dT%H:%M:%S.%f')
            else:
                value = datetime.strptime(value[0:-1], '%Y-%m-%dT%H:%M:%S')
        except:
            print(value)
            raise Exception('failed to convert line')
    return key, value


def entry_to_dict(entry_lines):
    dico = dict()
    for line in entry_lines:
        key, value = htlmline_to_keyvalue(line)
        if key is None:
            continue
        dico[key] = value
    return dico


def entries_to_dict(lines):
    entries = dict()
    nlines = len(lines)
    ii = 0
    i0 = None
    within = False
    while (True):
        if ii >= nlines:
            break
        if lines[ii].replace(' ', '').replace('\n', '') == '<entry>':
            if within:
                raise Exception('already within')
            i0 = ii
            within = True
        elif lines[ii].replace(' ', '').replace('\n', '') == '</entry>':
            if not within:
                raise Exception('not within')
            entry_loc = entry_to_dict(lines[i0 + 1:ii])
            entries[entry_loc['filename']] = entry_loc
            i0 = None
            within = False
        ii += 1

    return entries


def get_entries_from_request(http_request, verbose=0):
    start_time = time.time()

    # cmd
    lines = get_txt_from_httprequest(http_request, userpass_dict=copernicus_hub_dict, return_mode='txtlines',
                                     verbose=verbose)

    # subtitle line
    subtitle_line = [line for line in lines if '</subtitle>' in line]
    assert len(subtitle_line) == 1
    subtitle_line = subtitle_line[0].split('</subtitle>')[0].split('>')[-1]
    try:
        assert 'Displaying' in subtitle_line and 'total results' in subtitle_line
        values = subtitle_line.split('total results')[0].split('Displaying')[-1].replace('to', '').replace('of',
                                                                                                           '').split()
        assert len(values) == 3
        result_min, result_max, total_results = [int(el) for el in values]
    except:
        result_min = 0
        result_max = int(subtitle_line.split('Displaying')[-1].split('results')[0]) - 1
        total_results = result_max + 1

    if total_results > 0:
        # get entries
        entries = entries_to_dict(lines)
    else:
        entries = dict()

    if verbose > 0:
        print('%s seconds' % (time.time() - start_time))

    return entries, result_min, result_max, total_results


class CopernicusS2ProductParser:

    @staticmethod
    def get_product_info(product_list, properties_selection=None, verbose=0):

        assert len(product_list) > 0
        time0 = time.time()

        http_request_base = '"https://scihub.copernicus.eu/dhus/search?q='
        entries = dict()
        pool = multiprocessing.Pool(min(20, len(product_list)))
        for entries_loc, result_min_loc, result_max_loc, total_results_loc in pool.starmap(get_entries_from_request, [
            (http_request_base + product_id + '"', verbose) for product_id in product_list]):
            entries.update(entries_loc)

        if properties_selection is not None:
            entries = {key: {key1: value[key1] for key1 in properties_selection} for key, value in entries.items()}

        if verbose > 0:
            print('  -> Executed in %s seconds' % (time.time() - time0))

        return entries

    @staticmethod
    def request_scihub(start_date, end_date, footprint_wkt, date_type='ingestiondate', n_results_per_page=100, verbose=1):
        
        footprint_wkt = polygonshape_to_wkt(footprint_wkt)
        
        assert date_type in ['ingestiondate', 'beginPosition']
        request_elements = dict()
        request_elements['producttype'] = 'S2MSI1C'
        request_elements[date_type] = '[%s TO %s]' % (
        start_date.strftime('%Y-%m-%dT%H:%M:%S.%f')[0:-3] + 'Z', end_date.strftime('%Y-%m-%dT%H:%M:%S.%f')[0:-3] + 'Z')
        if footprint_wkt is not None:
            request_elements['footprint'] = '\\"Intersects({})\\"'.format(footprint_wkt)
        http_request_base = '"https://scihub.copernicus.eu/dhus/search?q={}&rows={}&start='.format(
            ' AND '.join(['%s:%s' % (key, value) for key, value in request_elements.items()]), n_results_per_page)

        # initial request
        entries, result_min, result_max, total_results = get_entries_from_request(http_request_base + '0"',
                                                                                  verbose=min(verbose - 1, 0))
        if total_results == 0:
            if verbose > 0:
                print('  -> Search returned no results')
            return entries

        if verbose > 0:
            print('  -> Got %d/%d results from request' % (result_max + 1, total_results))

        if result_max < total_results - 1:
            # get the following pages with parallel requests
            assert result_max == n_results_per_page - 1
            n_iter = int(np.ceil(total_results / n_results_per_page))
            assert n_iter > 1
            if verbose > 0:
                print('  -> Launching %d parallel requests' % (n_iter - 1))
            pool = multiprocessing.Pool(min(20, n_iter - 1))
            for i0, (entries_loc, result_min_loc, result_max_loc, total_results_loc) in enumerate(
                    pool.starmap(get_entries_from_request, \
                                 [(http_request_base + '%d"' % (100 * i_iter), min(verbose - 1, 0)) for i_iter in
                                  range(1, n_iter)])):
                assert total_results == total_results_loc
                assert result_min_loc == (i0 + 1) * 100
                assert result_max_loc == min(total_results_loc, (i0 + 2) * 100) - 1
                entries.update(entries_loc)
        else:
            assert result_max == total_results - 1
        return entries

    @staticmethod
    def search(start_date, end_date, date_type='ingestiondate', footprint_wkt=None, tile_id_post_selection=None, properties_selection=None,
               verbose=1):

        time0 = time.time()
        n_results_per_page = 100
        delta_days = 1

        entries = None
        for d_start, d_end in [
            (start_date + timedelta(x), start_date + timedelta(min(x + delta_days, (end_date - start_date).days))) for x
            in list(range(0, (end_date - start_date).days, delta_days))]:
            if verbose > 0:
                print('Requesting from %s to %s:' % (str(d_start), str(d_end)))
            if isinstance(footprint_wkt, list):
                for i, footprint in enumerate(footprint_wkt):
                    if verbose > 0:
                        print('Requesting footprint [%d/%d]' % (i, len(footprint_wkt)))
                    entries_loc = CopernicusS2ProductParser.request_scihub(d_start, d_end, footprint, date_type=date_type, n_results_per_page=n_results_per_page, verbose=verbose)
                    if entries is None:
                        entries = entries_loc
                    else:
                        entries.update(entries_loc)
            else:
                entries_loc = CopernicusS2ProductParser.request_scihub(d_start, d_end, footprint_wkt, date_type=date_type, n_results_per_page=n_results_per_page, verbose=verbose)
                if entries is None:
                    entries = entries_loc
                else:
                    entries.update(entries_loc)
        

        # select tiles
        if tile_id_post_selection is not None:
            for key, value in entries.items():
                if 'tileid' not in value:
                    print(key)
                    print(value)
                    raise Exception('product does not have tileid information')
            entries = {key: value for key, value in entries.items() if value['tileid'] in tile_id_post_selection}

        if properties_selection is not None:
            entries = {key: {key1: value[key1] for key1 in properties_selection} for key, value in entries.items()}

        if verbose > 0:
            print('-> Executed in %s seconds' % (time.time() - time0))

        return entries

def pretty_print_POST(req):
    """
    At this point it is completely built and ready
    to be fired; it is "prepared".

    However pay attention at the formatting used in 
    this function because it is programmed to be pretty 
    printed and may differ from the actual request.
    """
    print('{}\n{}\r\n{}\r\n\r\n{}'.format(
        '-----------START-----------',
        req.method + ' ' + req.url,
        '\r\n'.join('{}: {}'.format(k, v) for k, v in req.headers.items()),
        req.body,
    ))



class CopernicusS1ProductParser:

    @staticmethod
    def get_product_info(product_list, properties_selection=None, verbose=0):

        assert len(product_list) > 0
        time0 = time.time()

        http_request_base = '"https://scihub.copernicus.eu/dhus/search?q='
        entries = dict()
        pool = multiprocessing.Pool(min(20, len(product_list)))
        for entries_loc, result_min_loc, result_max_loc, total_results_loc in pool.starmap(get_entries_from_request, [
            (http_request_base + product_id + '"', verbose) for product_id in product_list]):
            entries.update(entries_loc)

        if properties_selection is not None:
            entries = {key: {key1: value[key1] for key1 in properties_selection} for key, value in entries.items()}

        if verbose > 0:
            print('  -> Executed in %s seconds' % (time.time() - time0))

        return entries

    @staticmethod
    def request_scihub(start_date, end_date, footprint_wkt, date_type='ingestiondate', n_results_per_page=100, verbose=1):
        
        footprint_wkt = polygonshape_to_wkt(footprint_wkt)
        
        assert date_type in ['ingestiondate', 'beginPosition']
        request_elements = dict()
        request_elements['producttype'] = 'GRD'
        request_elements[date_type] = '[%s TO %s]' % (
        start_date.strftime('%Y-%m-%dT%H:%M:%S.%f')[0:-3] + 'Z', end_date.strftime('%Y-%m-%dT%H:%M:%S.%f')[0:-3] + 'Z')
        if footprint_wkt is not None:
            request_elements['footprint'] = '\\"Intersects({})\\"'.format(footprint_wkt)
        http_request_base = '"https://scihub.copernicus.eu/dhus/search?q={}&rows={}&start='.format(
            ' AND '.join(['%s:%s' % (key, value) for key, value in request_elements.items()]), n_results_per_page)
            

        # initial request
        entries, result_min, result_max, total_results = get_entries_from_request(http_request_base + '0"',
                                                                                  verbose=min(verbose - 1, 0))
        if total_results == 0:
            if verbose > 0:
                print('  -> Search returned no results')
            return entries

        if verbose > 0:
            print('  -> Got %d/%d results from request' % (result_max + 1, total_results))

        if result_max < total_results - 1:
            # get the following pages with parallel requests
            assert result_max == n_results_per_page - 1
            n_iter = int(np.ceil(total_results / n_results_per_page))
            assert n_iter > 1
            if verbose > 0:
                print('  -> Launching %d parallel requests' % (n_iter - 1))
            pool = multiprocessing.Pool(min(20, n_iter - 1))
            for i0, (entries_loc, result_min_loc, result_max_loc, total_results_loc) in enumerate(
                    pool.starmap(get_entries_from_request, \
                                 [(http_request_base + '%d"' % (100 * i_iter), min(verbose - 1, 0)) for i_iter in
                                  range(1, n_iter)])):
                assert total_results == total_results_loc
                assert result_min_loc == (i0 + 1) * 100
                assert result_max_loc == min(total_results_loc, (i0 + 2) * 100) - 1
                entries.update(entries_loc)
        else:
            assert result_max == total_results - 1
        assert len(entries) == total_results, 'error: retrieved %d / %d product ids'%(len(entries), total_results)
            
        #remove products with polarisation != IW and polarisationmode != VV VH
        remove_list = []
        for s1_name, s1_dict in entries.items():
            if s1_dict['polarisationmode'] != 'VV VH' or s1_dict['swathidentifier'] != 'IW':
                remove_list.append(s1_name)
        for s1_name in remove_list:
            del entries[s1_name]
            
        return entries
        

    @staticmethod
    def search(start_date, end_date, date_type='ingestiondate', footprint_wkt=None, match_s2_shapes=None, properties_selection=None, verbose=1):

        time0 = time.time()
        n_results_per_page = 100
        delta_days = 1

        entries = None
        for d_start, d_end in [
            (start_date + timedelta(x), start_date + timedelta(min(x + delta_days, (end_date - start_date).days))) for x
            in list(range(0, (end_date - start_date).days, delta_days))]:
            if verbose > 0:
                print('Requesting from %s to %s:' % (str(d_start), str(d_end)))
            if isinstance(footprint_wkt, list):
                for i, footprint in enumerate(footprint_wkt):
                    if verbose > 0:
                        print('Requesting footprint [%d/%d]' % (i, len(footprint_wkt)))
                    entries_loc = CopernicusS1ProductParser.request_scihub(d_start, d_end, footprint, date_type=date_type, n_results_per_page=n_results_per_page, verbose=verbose)
                    if entries is None:
                        entries = entries_loc
                    else:
                        entries.update(entries_loc)
            else:
                entries_loc = CopernicusS1ProductParser.request_scihub(d_start, d_end, footprint_wkt, date_type=date_type, n_results_per_page=n_results_per_page, verbose=verbose)
                if entries is None:
                    entries = entries_loc
                else:
                    entries.update(entries_loc)
                                                                   
        if match_s2_shapes is not None:
            for s1_name, s1_dict in entries.items():
                entries[s1_name]['match_s2_shapes'] = get_s1_intersection_with_s2(s1_dict['footprint'], match_s2_shapes)

        if properties_selection is not None:
            entries = {key: {key1: value[key1] for key1 in properties_selection} for key, value in entries.items()}

        if verbose > 0:
            print('-> Executed in %s seconds' % (time.time() - time0))

        return entries


########################################
if __name__ == '__main__':
    entries = CopernicusS1ProductParser.search(datetime(2020, 5, 18, 14, 0, 0), datetime(2020, 5, 19, 20, 0, 0))
    set_trace()


