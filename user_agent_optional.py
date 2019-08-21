import scrapy
user_agents_version_3 = {
        'DOWNLOAD_DELAY': '30',
        # 'DEPTH_LIMIT': '100',
        # 'ITEM_PIPELINES': {
        #     'scrapy.pipelines.files.FilesPipeline': 1,
        #     'scrapy.pipelines.images.ImagesPipeline': 1
        # },
        # 'IMAGES_STORE': 'media',
        # 'IMAGES_THUMBS': { 'small': (50, 50) },
        # 'FILES_STORE': 'files',
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 6.1; rv:45.0) Gecko/20100101 Firefox/45.0',
        'TELNETCONSOLE_ENABLED': False,
        'DOWNLOAD_HANDLERS': {'s3': None}
    }    
