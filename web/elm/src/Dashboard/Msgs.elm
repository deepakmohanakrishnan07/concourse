module Dashboard.Msgs exposing (Msg(..))

import Concourse.Cli as Cli
import Concourse
import Dashboard.APIData as APIData
import Http
import Keyboard
import RemoteData
import Time
import Window


type Msg
    = Noop
    | APIDataFetched (RemoteData.WebData ( Time.Time, ( APIData.APIData, Maybe Concourse.User ) ))
    | ClockTick Time.Time
    | AutoRefresh Time.Time
    | ShowFooter
    | KeyPressed Keyboard.KeyCode
    | KeyDowns Keyboard.KeyCode
    | PipelinePauseToggled Concourse.Pipeline (Result Http.Error ())
    | DragStart String Int
    | DragOver String Int
    | DragEnd
    | Tooltip String String
    | TooltipHd String String
    | TogglePipelinePaused Concourse.Pipeline
    | PipelineButtonHover (Maybe Concourse.Pipeline)
    | CliHover (Maybe Cli.Cli)
    | ScreenResized Window.Size
    | TeamsFetched (RemoteData.WebData (List Concourse.Team))
    | LogIn
    | LogOut
    | LoggedOut (Result Http.Error ())
    | FilterMsg String
    | FocusMsg
    | BlurMsg
    | SelectMsg Int
    | ToggleUserMenu
    | ShowSearchInput
