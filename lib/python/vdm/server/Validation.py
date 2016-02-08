"""
This file is part of VoltDB.

Copyright (C) 2008-2016 VoltDB Inc.

This file contains original code and/or modifications of original code.
Any modifications made by VoltDB Inc. are licensed under the following
terms and conditions:

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
OTHER DEALINGS IN THE SOFTWARE.
"""

from wtforms.validators import DataRequired, IPAddress, ValidationError, Optional, Regexp
from flask_inputs import Inputs
import socket
import traceback
from flask_inputs.validators import JsonSchema


class Validation(object):
    """"Class to handle the validation."""

    @staticmethod
    def port_validation(form, field):
        """
        Port Validation part
        """
        response_result = {'status': 1}
        if ":" in field.data:
            count = field.data.count(":")
            if count > 1:
                raise ValidationError('Invalid value')
            array = field.data.split(":")
            if len(array) == 2:
                try:
                    socket.inet_pton(socket.AF_INET, array[0])
                except AttributeError:
                    print traceback.format_exc()
                    try:
                        socket.inet_aton(array[0])
                    except socket.error:
                        print traceback.format_exc()
                        raise ValidationError('Invalid IP address')
                    return array[0].count('.') == 3
                except socket.error:
                    print traceback.format_exc()
                    raise ValidationError('Invalid IP address')
                try:
                    val = int(array[1])
                    if val < 1 or val >= 65535:
                        raise ValidationError('Port must be greater than 1 and less than 65535')
                except ValueError as err:
                    msg = err.message
                    print traceback.format_exc()
                    if msg is 'Port must be greater than 1 and less than 65535':
                        raise ValidationError('Port must be greater than 1 and less than 65535')
                    else:
                        raise ValidationError('Value must be positive.')
            else:
                raise ValidationError('Invalid value')
        else:
            try:
                val = int(field.data)
                if val < 1 or val > 65536:
                    raise ValidationError('Port must be greater than 1 and less than 65535')
            except ValueError as err:
                msg = err.message
                print traceback.format_exc()
                if msg is 'Port must be greater than 1 and less than 65535':
                    raise ValidationError('Port must be greater than 1 and less than 65535')
                else:
                    raise ValidationError('Value must be positive.')


class ServerInputs(Inputs):
    """
    Validation class for inputs
    """
    json = {
        'name': [
            Optional(),
            Regexp('^[a-zA-Z0-9_.-]+$', 0, 'Only alphabets, numbers, _ and . are allowed.')
        ],
        'hostname': [
            DataRequired('Host name is required.'),
            Regexp('^[a-zA-Z0-9_.-]+$', 0, 'Only alphabets, numbers, _ and . are allowed.')
        ],
        'enabled': [
            Optional(),
        ],
        'admin-listener': [
            Optional(),
            Validation.port_validation
        ],
        'internal-listener': [
            Optional(),
            Validation.port_validation
        ],
        'http-listener': [
            Optional(),
            Validation.port_validation
        ],
        'zookeeper-listener': [
            Optional(),
            Validation.port_validation
        ],
        'replication-listener': [
            Optional(),
            Validation.port_validation
        ],
        'client-listener': [
            Optional(),
            Validation.port_validation
        ],
        'internal-interface': [
            Optional(),
            IPAddress('Invalid IP address.')
        ],
        'external-interface': [
            Optional(),
            IPAddress('Invalid IP address.')
        ],
        'public-interface': [
            Optional(),
            IPAddress('Invalid IP address.')
        ],
    }


user_schema = {
    "type": "object",
    "properties": {
            "databaseid":{
                "id": "databaseid",
                "type": "integer",
            },
            "name": {
                    "id": "name",
                    "type": "string",
                    "minLength": 1
                },
            "password": {
                "id": "password",
                "type": "string",
                "minLength": 1
            },
            "roles": {
                "id": "roles",
                "enum": ["Administrator", "User"]
            },
            "plaintext": {
                "id": "plaintext",
                "type": "boolean"
            }
    },
    "required": ["name", "password", "roles"]
}

schema = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "id": "/",
    "type": "object",
    "properties": {
        "databaseid": {
            "id": "databaseid",
            "type": "integer"
        },
        "heap": {
            "id": "heap",
            "type": "object",
            "properties": {
                "maxjavaheap": {
                    "id": "maxjavaheap",
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 32
                }
            }
        },
        "cluster": {
            "id": "cluster",
            "type": "object",
            "properties": {
                "hostcount": {
                    "id": "hostcount",
                    "type": "integer"
                },
                "sitesperhost": {
                    "id": "sitesperhost",
                    "type": "integer",
                    "minimum": 0,
                    "maximum": 15
                },
                "kfactor": {
                    "id": "kfactor",
                    "type": "integer",
                    "minimum": 0,
                    "maximum": 2
                },
                "elastic": {
                    "id": "elastic",
                    "type": "string"
                },
                "schema": {
                    "id": "schema",
                    "type": "string"
                }
            },
            "additionalProperties": False,
            # "required": ['sitesperhost']
        },
        "paths": {
            "id": "paths",
            "type": "object",
            "properties": {
                "voltdbroot": {
                    "id": "voltdbroot",
                    "type": "object",
                    "properties": {
                        "path": {
                            "id": "path",
                            "type": "string"
                        }
                    },
                    "additionalProperties": False
                },
                "snapshots": {
                    "id": "snapshots",
                    "type": "object",
                    "properties": {
                        "path": {
                            "id": "path",
                            "type": "string"
                        }
                    },
                    "additionalProperties": False
                },
                "exportoverflow": {
                    "id": "exportoverflow",
                    "type": "object",
                    "properties": {
                        "path": {
                            "id": "path",
                            "type": "string"
                        }
                    },
                    "additionalProperties": False
                },
                "droverflow": {
                    "id": "droverflow",
                    "type": "object",
                    "properties": {
                        "path": {
                            "id": "path",
                            "type": "string"
                        }
                    },
                    "additionalProperties": False
                },
                "commandlog": {
                    "id": "commandlog",
                    "type": "object",
                    "properties": {
                        "path": {
                            "id": "path",
                            "type": "string"
                        }
                    },
                    "additionalProperties": False
                },
                "commandlogsnapshot": {
                    "id": "commandlogsnapshot",
                    "type": "object",
                    "properties": {
                        "path": {
                            "id": "path",
                            "type": "string"
                        }
                    },
                    "additionalProperties": False
                }
            },
            "additionalProperties": False
        },
        "partition-detection": {
            "id": "partitionDetection",
            "type": "object",
            "properties": {
                "snapshot": {
                    "id": "snapshot",
                    "type": "object",
                    "properties": {
                        "prefix": {
                            "id": "prefix",
                            "type": "boolean"
                        }
                    },
                    "additionalProperties": False
                },
                "enabled": {
                    "id": "enabled",
                    "type": "boolean"
                }
            },
            "additionalProperties": False
        },
        "admin-mode": {
            "id": "admin-mode",
            "type": "object",
            "properties": {
                "port": {
                    "id": "port",
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 65536
                },
                "adminstartup": {
                    "id": "adminstartup",
                    "type": "boolean"
                }
            },
            "additionalProperties": False
        },
        "heartbeat": {
            "id": "heartbeat",
            "type": "object",
            "properties": {
                "timeout": {
                    "id": "timeout",
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 2147483647
                }
            },
            "additionalProperties": False
        },
        "httpd": {
            "id": "httpd",
            "type": "object",
            "properties": {
                "jsonapi": {
                    "id": "jsonapi",
                    "type": "object",
                    "properties": {
                        "enabled": {
                            "id": "enabled",
                            "type": "boolean"
                        }
                    },
                    "additionalProperties": False
                },
                "port": {
                    "id": "port",
                    "type": "integer"
                },
                "enabled": {
                    "id": "enabled",
                    "type": "boolean"
                }
            },
            "additionalProperties": False
        },
        "snapshot": {
            "id": "snapshot",
            "type": "object",
            "properties": {
                "frequency": {
                    "id": "frequency",
                    "type": "string"
                },
                "retain": {
                    "id": "retain",
                    "type": "integer"
                },
                "prefix": {
                    "id": "prefix",
                    "type": "string",
                    "pattern": "^[a-zA-Z0-9_.]+$"
                },
                "enabled": {
                    "id": "enabled",
                    "type": "boolean"
                }
            },
            "additionalProperties": False
        },
        "users": {
            "id": "users",
            "type": "object",
            "properties": {
                "user": {
                    "id": "user",
                    "type": "array",
                    "items": {
                        "anyOf": [
                            {
                                "type": "object",
                                "properties": {
                                    "name": {
                                        "id": "name",
                                        "type": "string",
                                        "minLength": 1
                                    },
                                    "password": {
                                        "id": "password",
                                        "type": "string",
                                        "minLength": 1
                                    },
                                    "roles": {
                                        "id": "roles",
                                        "type": "string",
                                        "enum": ["Administrator", "User"]
                                    },
                                    "plaintext": {
                                        "id": "plaintext",
                                        "type": "boolean"
                                    },

                                },
                                "required": ["name", "roles", "password"]
                            }
                        ]
                    }

                }
            }

        },
        "export": {
            "id": "export",
            "type": "object",
            "properties": {
                "configuration": {
                    "id": "configuration",
                    "type": "array",
                    "items": {
                        "anyOf": [
                            {
                                "type": "object",
                                "properties": {
                                    "property": {
                                        "id": "property",
                                        "type": "array",
                                        "items": {
                                            "anyOf": [
                                                {
                                                    "type": "object",
                                                    "properties": {
                                                        "name": {
                                                            "id": "name",
                                                            "type": "string"
                                                        },
                                                        "value": {
                                                            "id": "value",
                                                            "type": "string"
                                                        }
                                                    }
                                                }
                                            ]
                                        },
                                        "required": ["value"]
                                    },
                                    "stream": {
                                        "id": "stream",
                                        "type": "string",
                                        "pattern": "^[a-zA-Z0-9_.]+$"
                                    },
                                    "enabled": {
                                        "id": "enabled",
                                        "type": "boolean"
                                    },
                                    "type": {
                                        "id": "type",
                                        "type": "string",
                                        "enum": ["kafka", "elasticsearch", "http", "file", "rabbitmq", "jdbc", "custom"]
                                    },
                                    "exportconnectorclass": {
                                        "id": "exportconnectorclass",
                                        "type": "string"
                                    },

                                },
                             "required": ["stream", "type"]
                            }
                        ]
                    }

                }
            }

        },
        "import": {
            "id": "import",
            "type": "object",
            "properties": {
                "configuration": {
                    "id": "configuration",
                    "type": "array",
                    "items": {
                        "anyOf": [
                            {
                                "type": "object",
                                "properties": {
                                    "property": {
                                        "id": "property",
                                        "type": "array",
                                        "items": {
                                            "anyOf": [
                                                {
                                                    "type": "object",
                                                    "properties": {
                                                        "name": {
                                                            "id": "name",
                                                            "type": "string"
                                                        },
                                                        "value": {
                                                            "id": "value",
                                                            "type": "string"
                                                        }
                                                    }
                                                }
                                            ],
                                            "required": ["value"]
                                        }
                                    },
                                    "module": {
                                        "id": "module",
                                        "type": "string",
                                        "pattern": "^[a-zA-Z0-9_.]+$"
                                    },
                                    "enabled": {
                                        "id": "enabled",
                                        "type": "boolean"
                                    },
                                    "type": {
                                        "id": "type",
                                        "type": "string",
                                        "enum": ["kafka", "elasticsearch", "http", "file", "rabbitmq", "jdbc", "custom"]
                                    },
                                    "format": {
                                        "id": "format",
                                        "type": "string"
                                    },

                                },
                                 "required": ["module"]
                            }
                        ]
                    }

                }
            }

        },
        "commandlog": {
            "id": "commandlog",
            "type": "object",
            "properties": {
                "frequency": {
                    "id": "frequency",
                    "type": "object",
                    "properties": {
                        "time": {
                            "id": "time",
                            "type": "integer",
                            "minimum": 0,
                            "maximum": 1000
                        },
                        "transactions": {
                            "id": "transactions",
                            "type": "integer",
                            "minimum": 0,
                            "maximum": 2147483647
                        }
                    },
                    "additionalProperties": False
                },
                "synchronous": {
                    "id": "synchronous",
                    "type": "boolean"
                },
                "enabled": {
                    "id": "enabled",
                    "type": "boolean"
                },
                "logsize": {
                    "id": "logsize",
                    "type": "integer",
                    "minimum": 3,
                    "maximum": 3000
                }
            },
            "additionalProperties": False
        },
        "systemsettings": {
            "id": "systemsettings",
            "type": "object",
            "properties": {
                "temptables": {
                    "id": "temptables",
                    "type": "object",
                    "properties": {
                        "maxsize": {
                            "id": "maxsize",
                            "type": "integer",
                            "minimum": 1,
                            "maximum": 2147483647
                        }
                    },
                    "additionalProperties": False
                },
                "snapshot": {
                    "id": "snapshot",
                    "type": "object",
                    "properties": {
                        "priority": {
                            "id": "priority",
                            "type": "integer",
                            "minimum": 0,
                            "maximum": 10
                        }
                    },
                    "additionalProperties": False
                },
                "elastic": {
                    "id": "elastic",
                    "type": "object",
                    "properties": {
                        "duration": {
                            "id": "duration",
                            "type": "integer"
                        },
                        "throughput": {
                            "id": "throughput",
                            "type": "integer"
                        }
                    },
                    "additionalProperties": False
                },
                "query": {
                    "id": "query",
                    "type": "object",
                    "properties": {
                        "timeout": {
                            "id": "timeout",
                            "type": "integer",
                            "minimum": 0,
                            "maximum": 2147483647
                        }
                    },
                    "additionalProperties": False
                },
                "resourcemonitor": {
                    "id": "resourcemonitor",
                    "type": "object",
                    "properties": {
                        "memorylimit": {
                            "id": "memorylimit",
                            "type": "object",
                            "properties": {
                                "size": {
                                    "id": "size",
                                    "type": "string",
                                }
                            },
                            "additionalProperties": False
                        },
                        "disklimit": {
                            "id": "disklimit",
                            "type": "object",
                            "properties": {
                                "feature": {
                                    "id": "feature",
                                    "type": "array",
                                    "items": {
                                        "id": "1",
                                        "type": "object",
                                        "properties": {
                                            "name": {
                                                "id": "name",
                                                "type": "string"
                                            },
                                            "size": {
                                                "id": "size",
                                                "type": "string",
                                                "minimum": 0,
                                                "maximum": 2147483647
                                            }
                                        },
                                        "additionalProperties": False
                                    },
                                    "additionalItems": False
                                },
                                "size": {
                                    "id": "size",
                                    "type": "string"
                                }
                            },
                            "additionalProperties": False
                        },
                        "frequency": {
                            "id": "frequency",
                            "type": "integer"
                        }
                    },
                    "additionalProperties": False
                }
            },
            "additionalProperties": False
        },
        "security": {
            "id": "security",
            "type": "object",
            "properties": {
                "enabled": {
                    "id": "enabled",
                    "type": "boolean"
                },
                "provider": {
                    "id": "provider",
                    "type": "string"
                }
            },
            "additionalProperties": False
        },
        "dr": {
            "id": "dr",
            "type": "object",
            "properties": {
                "id": {
                    "id": "id",
                    "type": "integer",
                    "minimum": 0,
                    "maximum": 2147483647
                },
                "listen": {
                    "id": "listen",
                    "type": "boolean"
                },
                "port": {
                    "id": "port",
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 65535
                },
                "connection": {
                    "id": "connection",
                    "type": "object",
                    "properties": {
                        "source": {
                            "id": "source",
                            "type": "string",
                        }
                    }
                }
            }
        }
    },
    "additionalProperties": False
}


class JsonInputs(Inputs):
    json = [JsonSchema(schema=schema)]


class UserInputs(Inputs):
    json = [JsonSchema(schema= user_schema)]


class DatabaseInputs(Inputs):
    """
    Validation class for database inputs
    """
    json = {
        'name': [
            DataRequired('Database name is required.'),
            Regexp('^[a-zA-Z0-9_.]+$', 0, 'Only alphabets, numbers, _ and . are allowed.')
        ]
    }


class ConfigValidation(Inputs):
    """
    Validation class for ip address used to sync cluster.
    """
    json = {
        'ip_address': [
            DataRequired('IP address is required.'),
            IPAddress('Invalid IP address.')
        ]
    }
