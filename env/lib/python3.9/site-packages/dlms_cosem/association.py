from dlms_cosem.ber import BER
import dlms_cosem.protocol.dlms as dlms

"""
The AARQ, AARE has alot of functions and variations. Most of the classes here
are not perfect. I have just tried to get all cases down. I will start making
the classes better when I start working on the client. Then I will better see
what a good API would be.
"""


class UserInformation:
    tag = 0x04  # is encoded as an octetstring

    def __init__(self, initiate_request):
        self.initiate_request = initiate_request

        # TODO: when using ciphered apdus we will get other apdus. (33 64) global or
        #  dedicated cipered iniitate requests

    @classmethod
    def from_bytes(cls, _bytes):
        tag, length, data = BER.decode(_bytes)
        if tag != 0x04:
            raise ValueError(f'The tag for UserInformation data should be 0x04'
                             f'not {tag}')

        initiate_request = dlms.InitiateRequestAPDU.from_bytes(data)

        return cls(initiate_request=initiate_request)

    def to_bytes(self):
        return BER.encode(self.tag, self.initiate_request.to_bytes())

    def __repr__(self):
        return f'\n\t\tinitiate_request = {self.initiate_request}'


class DLMSObjectIdentifier:
    """
    The DLMS Association has been assigned a prefix for all of its OBJECT
    IDENDIFIERS
    """
    tag = 6
    prefix = b'\x60\x85\x74\x05\x08'


class AppContextName(DLMSObjectIdentifier):
    """
    This defines how to reference objects in the meter and if ciphered APDU:s
    are allowed.

    """
    # TODO: Can this be a bit more generalized??
    app_context = 1

    valid_context_ids = [1, 2, 3, 4]

    def __init__(self, logical_name_refs=True, ciphered_apdus=True):

        self.logical_name_refs = logical_name_refs
        self.ciphered_apdus = ciphered_apdus
        self.context_id = self.calculate_context_id()

    @classmethod
    def from_bytes(cls, _bytes):
        tag, length, data = BER.decode(_bytes)

        if tag != DLMSObjectIdentifier.tag:
            raise ValueError(f'Tag of {tag} is not a valid tag for '
                             f'ObjectIdentifiers')

        context_id = data[-1]
        if context_id not in AppContextName.valid_context_ids:
            raise ValueError(f'context_id of {context_id} is not valid')

        total_prefix = bytes(data[:-1])
        if total_prefix != (DLMSObjectIdentifier.prefix +
                            bytes([AppContextName.app_context])):
            raise ValueError(f'Static part of object id it is not correct'
                             f' according to DLMS: {total_prefix}')
        settings_dict = AppContextName.get_settings_by_context_id(context_id)
        return cls(**settings_dict)

    def to_bytes(self):
        total_data = self.prefix + bytes([self.app_context, self.context_id])
        return BER.encode(self.tag, total_data)

    def calculate_context_id(self):
        if self.logical_name_refs and not self.ciphered_apdus:
            return 1
        elif not self.logical_name_refs and not self.ciphered_apdus:
            return 2
        elif self.logical_name_refs and self.ciphered_apdus:
            return 3
        elif not self.logical_name_refs and self.ciphered_apdus:
            return 4

    @staticmethod
    def get_settings_by_context_id(context_id):
        settings_dict = {
            1: {'logical_name_refs': True, 'ciphered_apdus': False},
            2: {'logical_name_refs': False, 'ciphered_apdus': False},
            3: {'logical_name_refs': True, 'ciphered_apdus': True},
            4: {'logical_name_refs': False, 'ciphered_apdus': True},
        }
        return settings_dict.get(context_id)

    def __repr__(self):
        return (f'AppContextName \n'
                f'\t\t logical_name_refs = {self.logical_name_refs} \n'
                f'\t\t ciphered_apdus = {self.ciphered_apdus}')


class MechanismName(DLMSObjectIdentifier):
    app_context = 2
    valid_mechanism_ids = [0, 1, 2, 3, 4, 5, 6, 7]  #TODO: just check .keys(

    # TODO: Don't like this but can't be bothered to come up with other way now.
    mechanism_names_by_id = {
        0: 'none',  # lowest level
        1: 'lls',  # low level security
        2: 'hls',  # high level security
        3: 'hls-md5',  # HLS with MD5 , not recommended for new meters
        4: 'hls-sha1',  # HLS with SHA1 , not recommended for new meters
        5: 'hls-gmac',
        6: 'hls-sha256',
        7: 'hls-ecdsa',
    }
    id_by_mechanism_names = {
        'none': 0,  # lowest level
        'lls': 1,  # low level security
        'hls': 2,  # high level security
        'hls-md5': 3,  # HLS with MD5 , not recommended for new meters
        'hls-sha1': 4,  # HLS with SHA1 , not recommended for new meters
        'hls-gmac': 5,
        'hls-sha256': 6,
        'hls-ecdsa': 7,
    }

    def __init__(self, mechanism_name='none'):
        self.mechanism_name = mechanism_name
        self.mechanism_id = self.id_by_mechanism_names[mechanism_name]

    @classmethod
    def from_bytes(cls, _bytes):
        """
        Apparently the data in mechanism name is not encoded in BER.
        """

        mechanism_id = _bytes[-1]

        if mechanism_id not in MechanismName.valid_mechanism_ids:
            raise ValueError(f'mechanism_id of {mechanism_id} is not valid')

        total_prefix = bytes(_bytes[:-1])
        if total_prefix != (DLMSObjectIdentifier.prefix +
                            bytes([MechanismName.app_context])):
            raise ValueError(f'Static part of object id it is not correct'
                             f' according to DLMS: {total_prefix}')

        return cls(
            mechanism_name=MechanismName.mechanism_names_by_id[mechanism_id]
        )

    def to_bytes(self):
        total_data = self.prefix + bytes([self.app_context, self.mechanism_id])
        return total_data

    def __repr__(self):
        return self.mechanism_name


class AuthenticationValue:
    """
    Holds "password" in the AARQ and AARE
    Can either hold a charstring or a bitstring
    """

    password_types = ['chars', 'bits']

    def __init__(self, password=b'', _type='chars'):
        self.password = password
        if _type in self.password_types:
            self._type = _type

        else:
            raise ValueError(f'{_type} is not a valid auth value type')

    def __repr__(self):
        return f'{self._type}: {self.password}'

    @classmethod
    def from_bytes(cls, _bytes):
        tag, length, data = BER.decode(_bytes)
        if tag == 0x80:
            password_type = 'chars'
        elif tag == 0x80:
            password_type = 'bits'
        else:
            raise ValueError(f'Tag {tag} is not vaild for password')

        return cls(password=data, _type=password_type)

    def to_bytes(self):
        if self._type == 'chars':
            return BER.encode(0x80, self.password)
        elif self._type == 'bits':
            return BER.encode(0x81, self.password)






class AuthFunctionalUnit:
    """
    Consists of 2 bytes. First byte encodes the number of unused bytes in
    the second byte.
    So really you just need to set the last bit to 0 to use authentication.
    In the green book they use the 0x07 as first byte and 0x80 as last byte.
    We will use this to not make it hard to look up.
    It is a bit weirdly defined in the Green Book. I interpret is as if the data
    exists it is the functional unit 0 (authentication). In examples in the
    Green Book they set 0x070x80 as exists.

    """

    def __init__(self, authentication=False):
        self.authentication = authentication

    @classmethod
    def from_bytes(cls, _bytes):
        if len(_bytes) != 2:
            raise ValueError(f'Authentication Functional Unit data should by 2 '
                             f'bytes. Got: {_bytes}')
        last_byte = _bytes[-1]
        # should I check anything?
        return cls(authentication=True)

    def to_bytes(self):
        if self.authentication:
            return b'\x07\x80'
        else:
            # when not using authentication this the sender-acse-requirements
            # should not be in the data.
            return None

    def __repr__(self):
        if self.authentication:
            return 'Authentication = True'
        else:
            return 'Authentication = False'


class AARQAPDU():
    """
      AARQ_apdu ::= [APPLICATION 0] IMPLICIT SEQUENCE {
      protocol_version [0] IMPLICIT BIT STRING OPTIONAL,
      application_context_name          [1]  EXPLICIT OBJECT IDENTIFIER,
      called_AP_title                   [2]  AP_title OPTIONAL,
      called_AE_qualifier               [3]  AE_qualifier OPTIONAL,
      called_AP_invocation_identifier   [4]  EXPLICIT AP_invocation_identifier OPTIONAL,
      called_AE_invocation_identifier   [5]  EXPLICIT AE_invocation_identifier OPTIONAL,
      calling_AP_title                  [6]  AP_title OPTIONAL,
      calling_AE_qualifier              [7]  AE_qualifier OPTIONAL,
      calling_AP_invocation_identifier  [8]  AP_invocation_identifier OPTIONAL,
      calling_AE_invocation_identifier  [9]  AE_invocation_identifier OPTIONAL,
      --  The following field shall not be present if only the Kernel is used.
      sender_acse_requirements          [10] IMPLICIT ACSE_requirements OPTIONAL,
      --  The following field shall only be present if the Authentication functional unit is selected.
      mechanism_name                    [11] IMPLICIT Mechanism_name OPTIONAL,
      --  The following field shall only be present if the Authentication functional unit is selected.
      calling_authentication_value      [12] EXPLICIT Authentication_value OPTIONAL,
      application_context_name_list
        [13] IMPLICIT Application_context_name_list OPTIONAL,
      --  The above field shall only be present if the Application Context Negotiation functional unit is selected
      implementation_information        [29] IMPLICIT Implementation_data OPTIONAL,
      user_information [30] EXPLICIT Association_information OPTIONAL
    }
    """

    tag = 0x60  # Application 0 = 60H = 96
    # TODO: Use NamedTuple
    tags = {
        0x80: ('protocol_version', None),  # Context specific, constructed? 0
        0xa1: ('application_context_name', AppContextName),
        162: ('called_ap_title', None),
        163: ('called_ae_qualifier', None),
        164: ('called_ap_invocation_identifier', None),
        165: ('called_ae_invocation_identifier', None),
        166: ('calling_ap_title', None),
        167: ('calling_ae_qualifier', None),
        168: ('calling_ap_invocation_identifier', None),
        169: ('calling_ae_invocation_identifier', None),
        0x8a: ('sender_acse_requirements', AuthFunctionalUnit),
        0x8b: ('mechanism_name', MechanismName),
        0xac: ('calling_authentication_value', AuthenticationValue),
        0xbd: ('implementation_information', None),
        0xbe: ('user_information', UserInformation)  # Context specific, constructed 30
    }

    def __init__(self,
                 protocol_version=1,
                 application_context_name=None,
                 called_ap_title=None,
                 called_ae_qualifier=None,
                 called_ap_invocation_identifier=None,
                 called_ae_invocation_identifier=None,
                 calling_ap_title=None,
                 calling_ae_qualifier=None,
                 calling_ap_invocation_identifier=None,
                 calling_ae_invocation_identifier=None,
                 sender_acse_requirements=None,
                 mechanism_name=None,
                 calling_authentication_value=None,
                 implementation_information=None,
                 user_information=None,
                 raw_bytes=None):

        self.protocol_version = protocol_version
        self.application_context_name = application_context_name
        self.called_ap_title = called_ap_title
        self.called_ae_qualifier = called_ae_qualifier
        self.called_ap_invocation_identifier = called_ap_invocation_identifier
        self.called_ae_invocation_identifier = called_ae_invocation_identifier
        self.calling_ap_title = calling_ap_title
        self.calling_ae_qualifier = calling_ae_qualifier
        self.calling_ap_invocation_identifier = calling_ap_invocation_identifier
        self.calling_ae_invocation_identifier = calling_ae_invocation_identifier

        # if this is present authentication is used.
        self.sender_acse_requirements = sender_acse_requirements
        # these 2should not be present if authentication is not used.
        self.mechanism_name = mechanism_name
        self.calling_authentication_value = calling_authentication_value

        self.implementation_information = implementation_information
        self.user_information = user_information

        self._raw_bytes = raw_bytes

    @classmethod
    def from_bytes(cls, aarq_bytes):
        # put it in a bytearray to be able to pop.
        aarq_data = bytearray(aarq_bytes)

        aarq_tag = aarq_data.pop(0)
        if not aarq_tag == cls.tag:
            raise ValueError('Bytes are not an AARQ APDU. TAg is not int(96)')

        aarq_length = aarq_data.pop(0)

        if not len(aarq_data) == aarq_length:
            raise ValueError('The APDU Data lenght does not correspond '
                             'to length byte')

        # Assumes that the protocol-version is 1 and we don't need to decode it

        # Decode the AARQ  data
        object_dict = dict()
        object_dict['raw_bytes'] = aarq_bytes

        # use the data in tags to go through the bytes and create objects.
        while True:
            # TODO: this does not take into account when defining objects in dict and not using them.
            object_tag = aarq_data.pop(0)
            object_desc = AARQAPDU.tags.get(object_tag, None)
            if object_desc is None:
                raise ValueError(f'Could not find object with tag {object_tag} '
                                 f'in AARQ definition')

            object_length = aarq_data.pop(0)
            object_data = bytes(aarq_data[:object_length])
            aarq_data = aarq_data[object_length:]

            object_name = object_desc[0]
            object_class = object_desc[1]

            if object_class is not None:
                object_data = object_class.from_bytes(object_data)

            object_dict[object_name] = object_data

            if len(aarq_data) <= 0:
                break

        return cls(**object_dict)

    def to_bytes(self):
        # if we created the object from bytes we can just return the same bytes
        # if self._raw_bytes is not None:
        #    return self._raw_bytes
        aarq_data = bytearray()
        # default value of protocol_version is 1. Only decode if other than 1
        if self.protocol_version != 1:
            aarq_data.extend(
                BER.encode(160, bytes(self.protocol_version))
            )
        if self.application_context_name is not None:
            aarq_data.extend(
                BER.encode(161, self.application_context_name.to_bytes())
            )
        if self.called_ap_title is not None:
            aarq_data.extend(
                BER.encode(162, self.called_ap_title)
            )
        if self.called_ae_qualifier is not None:
            aarq_data.extend(
                BER.encode(163, self.called_ae_qualifier)
            )
        if self.called_ap_invocation_identifier is not None:
            aarq_data.extend(
                BER.encode(164, self.called_ap_invocation_identifier)
            )
        if self.called_ae_invocation_identifier is not None:
            aarq_data.extend(
                BER.encode(165, self.called_ae_invocation_identifier)
            )
        if self.calling_ap_title is not None:
            aarq_data.extend(
                BER.encode(166, self.calling_ap_title)
            )
        if self.calling_ae_qualifier is not None:
            aarq_data.extend(
                BER.encode(167, self.calling_ae_qualifier)
            )
        if self.calling_ap_invocation_identifier is not None:
            aarq_data.extend(
                BER.encode(168, self.calling_ap_invocation_identifier)
            )
        if self.calling_ae_invocation_identifier is not None:
            aarq_data.extend(
                BER.encode(169, self.calling_ae_invocation_identifier)
            )
        if self.sender_acse_requirements is not None:
            aarq_data.extend(
                BER.encode(0x8a, self.sender_acse_requirements.to_bytes())
            )
        if self.mechanism_name is not None:
            aarq_data.extend(
                BER.encode(0x8b, self.mechanism_name.to_bytes())
            )
        if self.calling_authentication_value is not None:
            aarq_data.extend(
                BER.encode(0xac, self.calling_authentication_value.to_bytes())
            )
        if self.implementation_information is not None:
            aarq_data.extend(
                BER.encode(0xbd, self.implementation_information)
            )
        if self.user_information is not None:
            aarq_data.extend(
                BER.encode(0xbe, self.user_information.to_bytes())
            )
        # TODO: UPDATE THE ENCODING TAGS!

        return BER.encode(self.tag, bytes(aarq_data))

        # TODO: make BER.encode handle bytes or bytearray to save code space.
        # TODO: CAn we use an orderedDict to loopt through all elemetns of the aarq to be transformed.

        # TODO: Add encoding of all values from ground up.

    def __repr__(self):
        return (
            f'AARQ APDU \n'
            f'\t protocol_version = {self.protocol_version} \n'
            f'\t application_context_name = {self.application_context_name} \n'
            f'\t called_ap_title = {self.called_ap_title} \n'
            f'\t called_ae_qualifier = {self.called_ae_qualifier} \n'
            f'\t called_ap_invocation_identifier = '
            f'{self.called_ap_invocation_identifier} \n'
            f'\t called_ae_invocation_identifier = '
            f'{self.called_ae_invocation_identifier} \n'
            f'\t calling_ap_title = {self.calling_ap_title} \n'
            f'\t calling_ae_qualifier = {self.calling_ae_qualifier} \n'
            f'\t calling_ap_invocation_identifier = '
            f'{self.calling_ap_invocation_identifier} \n'
            f'\t calling_ae_invocation_identifier = '
            f'{self.calling_ae_invocation_identifier} \n'
            f'\t sender_acse_requirements = {self.sender_acse_requirements} \n'
            f'\t mechanism_name: {self.mechanism_name} \n'
            f'\t calling_authentication_value = '
            f'{self.calling_authentication_value} \n'
            f'\t implementation_information = '
            f'{self.implementation_information}\n'
            f'\t user_information = {self.user_information}'
        )
